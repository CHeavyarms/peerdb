package connpostgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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
// joins a map from the relids of the parent and all its descendants to their schemas, which plans as a hash join,
// where a correlated lookup per row multiplied the scan's cost. Within the map, each schema is looked up by index:
// joining pg_class let the planner hash every relation in the catalog. The map must cover every descendant the query
// returns without ONLY, whichever of them CDC routes, or their rows would be stamped NULL.
// Queries using it must qualify their column references: see tableColumns and watermarkColumn.
func (s snapshotSourceSchema) fromClause(table string) string {
	if !s.enabled {
		return table
	}
	from := table + " AS " + snapshotSourceAlias
	if s.perRow {
		from += fmt.Sprintf(" LEFT JOIN (WITH RECURSIVE d(oid) AS (SELECT %[1]d::oid"+
			" UNION SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN d ON i.inhparent = d.oid)"+
			" SELECT d.oid AS _peerdb_relid, (SELECT n.nspname::text FROM pg_catalog.pg_class c"+
			" JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = d.oid) AS _peerdb_nspname FROM d)"+
			" AS %[2]s ON %[2]s._peerdb_relid = %[3]s.tableoid",
			s.mirroredOID, snapshotSchemaMapAlias, snapshotSourceAlias)
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

// reservedSourceSchemaColumnQuery finds tables among the mirrored ones ($1 schemas, $2 names) and all their
// inheritance descendants or partitions that have a column named like the source schema column ($3). It follows
// every pg_inherits edge, deliberately more than CDC routing does, as the snapshot reads every descendant. The
// planner overestimates the closure, so LIMIT 1 keeps the per-relation probe from becoming a scan of the whole
// pg_attribute index, and MATERIALIZED keeps it from joining every relation to pg_class first; the probes run in
// OID order for locality.
const reservedSourceSchemaColumnQuery = `
	WITH RECURSIVE mirrored AS (
		SELECT c.oid
		FROM unnest($1::text[], $2::text[]) AS t(nspname, relname)
		JOIN pg_catalog.pg_namespace n ON n.nspname = t.nspname
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.relname
	), candidates AS (
		SELECT oid FROM mirrored
		UNION
		SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN candidates t ON i.inhparent = t.oid
	), offending AS MATERIALIZED (
		SELECT t.oid
		FROM (SELECT oid FROM candidates ORDER BY oid) t
		CROSS JOIN LATERAL (
			SELECT FROM pg_catalog.pg_attribute a
			WHERE a.attrelid = t.oid AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped
			LIMIT 1
		) a
	)
	SELECT n.nspname, c.relname
	FROM offending o
	JOIN pg_catalog.pg_class c ON c.oid = o.oid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	ORDER BY 1, 2`

// checkReservedSourceSchemaColumn rejects mirrored tables, and their descendants or partitions, that have a
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
