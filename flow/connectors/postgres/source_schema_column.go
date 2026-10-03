package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// snapshotSourceAlias names the mirrored table in generated snapshot queries that project the source schema, and
// snapshotSchemaMapAlias the map of its hierarchy's schemas joined to it on tableoid.
const (
	snapshotSourceAlias    = "_peerdb_src"
	snapshotSchemaMapAlias = "_peerdb_map"
)

// snapshotSourceSchema describes how a snapshot query projects internal.SourceSchemaColumnName.
type snapshotSourceSchema struct {
	mirroredSchema string
	mirroredOID    uint32
	// project the column at all (internal.SnapshotProjectsSourceSchema, ClickHouse destination)
	enabled bool
	// the mirrored table is a plain table with inheritance children: each row carries the schema of the table
	// it is stored in. Otherwise every row carries mirroredSchema, the value ClickHouse used to stamp itself.
	perRow bool
}

func (s snapshotSourceSchema) quotedColumn() string {
	return common.QuoteIdentifier(internal.SourceSchemaColumnName)
}

// fromClause is the FROM target of a generated query on the mirrored table itself. For an inheritance parent it
// joins a small map from the relids of the parent and its direct children to their schemas, a hash join in the
// measured plans, where a correlated lookup per row multiplied the scan's cost (2M rows over 500 children: 0.68s
// against 6.8s). The map is read in the query's own snapshot, so it covers exactly the children whose rows the
// query returns. Deeper descendants find no entry; mirrors stamping child schemas reject such hierarchies.
// Queries using it must qualify their column references: see tableColumns and watermarkColumn.
func (s snapshotSourceSchema) fromClause(table string) string {
	if !s.enabled {
		return table
	}
	from := table + " AS " + snapshotSourceAlias
	if s.perRow {
		from += fmt.Sprintf(" LEFT JOIN (SELECT i.inhrelid AS _peerdb_relid, n.nspname::text AS _peerdb_nspname"+
			" FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid"+
			" JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE i.inhparent = %[1]d"+
			" UNION ALL SELECT %[1]d::oid, %[2]s::text) AS %[3]s ON %[3]s._peerdb_relid = %[4]s.tableoid",
			s.mirroredOID, utils.QuoteLiteral(s.mirroredSchema), snapshotSchemaMapAlias, snapshotSourceAlias)
	}
	return from
}

// tableColumns is the column list of a generated query on the mirrored table itself. Built with fromClause, every
// column is qualified, so source columns cannot clash with the schema map's.
func (s snapshotSourceSchema) tableColumns(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		if s.enabled {
			quoted = append(quoted, snapshotSourceAlias+"."+common.QuoteIdentifier(column))
		} else {
			quoted = append(quoted, common.QuoteIdentifier(column))
		}
	}
	return strings.Join(quoted, ",")
}

// watermarkColumn qualifies the quoted watermark column for a query built with fromClause: once the FROM clause has
// more than one item, system columns such as ctid are only found qualified. It qualifies whenever the projection is
// enabled, also with a single FROM item, so the queries stay valid if fromClause gains a join.
func (s snapshotSourceSchema) watermarkColumn(quoted string) string {
	if !s.enabled {
		return quoted
	}
	return snapshotSourceAlias + "." + quoted
}

// tableSelectList is the select list of a generated query on the mirrored table itself, which also returns the
// rows of its inheritance children.
func (s snapshotSourceSchema) tableSelectList(selected string) string {
	if !s.enabled {
		return selected
	}
	if selected == "*" {
		selected = snapshotSourceAlias + ".*"
	}
	if s.perRow {
		return selected + ", " + snapshotSchemaMapAlias + "._peerdb_nspname AS " + s.quotedColumn()
	}
	return selected + ", " + utils.QuoteLiteral(s.mirroredSchema) + "::text AS " + s.quotedColumn()
}

// childSelectList is the select list of a query reading one table of the hierarchy with ONLY. Every query of a
// partition must project the column the same way, since the stream schema is taken from the first one.
func (s snapshotSourceSchema) childSelectList(selected string, childSchema string) string {
	if !s.enabled {
		return selected
	}
	schema := s.mirroredSchema
	if s.perRow {
		schema = childSchema
	}
	return selected + ", " + utils.QuoteLiteral(schema) + "::text AS " + s.quotedColumn()
}

// snapshotSourceSchemaFor decides how the snapshot of table projects the source schema column.
func (c *PostgresConnector) snapshotSourceSchemaFor(
	ctx context.Context,
	config *protos.QRepConfig,
	dstType protos.DBType,
	table *common.QualifiedTable,
) (snapshotSourceSchema, error) {
	if dstType != protos.DBType_CLICKHOUSE {
		return snapshotSourceSchema{}, nil
	}
	projects, err := internal.SnapshotProjectsSourceSchema(ctx, config)
	if err != nil || !projects {
		return snapshotSourceSchema{}, err
	}

	handleInheritance, err := internal.PeerDBPostgresCDCHandleInheritanceForNonPartitionedTables(ctx, config.Env)
	if err != nil {
		return snapshotSourceSchema{}, err
	}
	var oid uint32
	var relkind byte
	var hasSubclass bool
	if err := c.conn.QueryRow(ctx, `SELECT c.oid, c.relkind, c.relhassubclass
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, table.Namespace, table.Table,
	).Scan(&oid, &relkind, &hasSubclass); err != nil {
		return snapshotSourceSchema{}, fmt.Errorf("failed to classify %s for source schema column: %w", table, err)
	}

	return snapshotSourceSchema{
		enabled: true,
		// declarative partition roots keep the root's schema: with publish_via_partition_root CDC cannot know
		// the leaf, and snapshot and CDC have to stamp the same value
		perRow:         handleInheritance && relkind == 'r' && hasSubclass,
		mirroredSchema: table.Namespace,
		mirroredOID:    oid,
	}, nil
}

// isExcludedColumn reports whether a source column is left out of replicated rows: excluded in the table mapping,
// or carrying the name PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN reserves for its own column.
func isExcludedColumn(mapping model.SourceTableMapping, sourceSchemaAsDestinationColumn bool, column string) bool {
	if _, ok := mapping.Exclude[column]; ok {
		return true
	}
	return sourceSchemaAsDestinationColumn && column == internal.SourceSchemaColumnName
}

// reservedSourceSchemaColumnQuery finds tables among the mirrored ones ($1 schemas, $2 names) and their direct
// inheritance children or partitions that have a column named like the source schema column ($3).
const reservedSourceSchemaColumnQuery = `
	WITH mirrored AS (
		SELECT c.oid
		FROM unnest($1::text[], $2::text[]) AS t(nspname, relname)
		JOIN pg_catalog.pg_namespace n ON n.nspname = t.nspname
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.relname
	), candidates AS (
		SELECT oid FROM mirrored
		UNION
		SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN mirrored m ON i.inhparent = m.oid
	)
	SELECT DISTINCT n.nspname, c.relname
	FROM candidates t
	JOIN pg_catalog.pg_class c ON c.oid = t.oid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
	WHERE a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY 1, 2`

// unsupportedInheritanceForChildSchemaQuery finds mirrored plain tables ($1 schemas, $2 names) whose inheritance
// hierarchy cannot be stamped consistently with each child's schema: descendants deeper than one level (CDC does
// not replicate them, the snapshot does) or children of more than one mirrored table.
const unsupportedInheritanceForChildSchemaQuery = `
	WITH mirrored AS (
		SELECT c.oid
		FROM unnest($1::text[], $2::text[]) AS t(nspname, relname)
		JOIN pg_catalog.pg_namespace n ON n.nspname = t.nspname
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.relname
		WHERE c.relkind = 'r'
	), problems AS (
		SELECT child.inhparent AS oid, 'has descendants more than one level deep' AS reason
		FROM pg_catalog.pg_inherits child
		JOIN pg_catalog.pg_inherits grandchild ON grandchild.inhparent = child.inhrelid
		WHERE child.inhparent IN (SELECT oid FROM mirrored)
		UNION
		SELECT i.inhparent, 'has a child that also inherits from another mirrored table'
		FROM pg_catalog.pg_inherits i
		WHERE i.inhparent IN (SELECT oid FROM mirrored)
		AND i.inhrelid IN (
			SELECT o.inhrelid FROM pg_catalog.pg_inherits o
			WHERE o.inhparent IN (SELECT oid FROM mirrored)
			GROUP BY o.inhrelid HAVING count(*) > 1
		)
	)
	SELECT DISTINCT n.nspname, c.relname, p.reason
	FROM problems p
	JOIN pg_catalog.pg_class c ON c.oid = p.oid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	ORDER BY 1, 2, 3`

// checkReservedSourceSchemaColumn rejects mirrored tables, and their direct children or partitions, that have a
// column named like the source schema column while PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN is on: its values
// would be silently replaced by the source schema.
func (c *PostgresConnector) checkReservedSourceSchemaColumn(
	ctx context.Context, env map[string]string, tables []*common.QualifiedTable,
) error {
	if enabled, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, env); err != nil || !enabled {
		return err
	}
	schemas, names := splitQualifiedTables(tables)
	rows, err := c.conn.Query(ctx, reservedSourceSchemaColumnQuery, schemas, names, internal.SourceSchemaColumnName)
	if err != nil {
		return fmt.Errorf("failed to check for columns named %s: %w", internal.SourceSchemaColumnName, err)
	}
	offending, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var schema, table string
		err := row.Scan(&schema, &table)
		return common.QuoteIdentifier(schema) + "." + common.QuoteIdentifier(table), err
	})
	if err != nil {
		return fmt.Errorf("failed to read tables with columns named %s: %w", internal.SourceSchemaColumnName, err)
	}
	if len(offending) > 0 {
		return fmt.Errorf("column %s is reserved while PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN is enabled, "+
			"rename it in: %s", internal.SourceSchemaColumnName, strings.Join(offending, ", "))
	}
	return nil
}

// checkInheritanceForChildSchema rejects inheritance hierarchies whose rows cannot all be stamped with their own
// table's schema, when a mirror would do so (setting on, internal version, inheritance handling on).
func (c *PostgresConnector) checkInheritanceForChildSchema(
	ctx context.Context, env map[string]string, version uint32, tables []*common.QualifiedTable,
) error {
	if version < shared.InternalVersion_SourceSchemaFromInheritanceChild {
		return nil
	}
	if enabled, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, env); err != nil || !enabled {
		return err
	}
	if enabled, err := internal.PeerDBPostgresCDCHandleInheritanceForNonPartitionedTables(ctx, env); err != nil || !enabled {
		return err
	}
	schemas, names := splitQualifiedTables(tables)
	rows, err := c.conn.Query(ctx, unsupportedInheritanceForChildSchemaQuery, schemas, names)
	if err != nil {
		return fmt.Errorf("failed to check inheritance hierarchies: %w", err)
	}
	problems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var schema, table, reason string
		err := row.Scan(&schema, &table, &reason)
		return common.QuoteIdentifier(schema) + "." + common.QuoteIdentifier(table) + " " + reason, err
	})
	if err != nil {
		return fmt.Errorf("failed to read inheritance hierarchies: %w", err)
	}
	if len(problems) > 0 {
		return errors.New("PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN stamps each inheritance child's rows with " +
			"its own schema, which needs every child to have exactly one mirrored parent and no children of its own: " +
			strings.Join(problems, "; "))
	}
	return nil
}
