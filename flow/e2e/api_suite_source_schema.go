package e2e

import (
	"fmt"
	"strconv"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Validation of mirrors using PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN on Postgres sources: the column name it
// adds is reserved, and stamping each inheritance child's schema needs single-level, single-parent hierarchies.

func (s APITestSuite) sourceSchemaMirror(name string, tables ...string) *protos.FlowConnectionConfigs {
	tableNameMapping := make(map[string]string, len(tables))
	for _, table := range tables {
		tableNameMapping[AttachSchema(s, table)] = table
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      name + "_" + s.suffix,
		TableNameMapping: tableNameMapping,
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true"}
	return flowConnConfig
}

func (s APITestSuite) requireValidationError(flowConnConfig *protos.FlowConnectionConfigs, message string) {
	s.t.Helper()
	_, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), message)
}

func (s APITestSuite) TestValidateCDCMirror_ReservedSourceSchemaColumn() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	exec := func(format string, args ...any) {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(format, args...)))
	}

	exec(`CREATE TABLE %s (id INT PRIMARY KEY, _peerdb_source_schema TEXT)`, AttachSchema(s, "reserved_on_table"))
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, AttachSchema(s, "reserved_on_child"))
	exec(`CREATE TABLE %s_tenant (_peerdb_source_schema TEXT) INHERITS (%s)`,
		AttachSchema(s, "reserved_on_child"), AttachSchema(s, "reserved_on_child"))
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, AttachSchema(s, "reserved_renamed"))

	for _, resync := range []bool{false, true} {
		onTable := s.sourceSchemaMirror("reserved_on_table", "reserved_on_table")
		onTable.Resync = resync
		s.requireValidationError(onTable, "column _peerdb_source_schema is reserved")

		onChild := s.sourceSchemaMirror("reserved_on_child", "reserved_on_child")
		onChild.Resync = resync
		s.requireValidationError(onChild, "reserved_on_child_tenant")

		renamed := s.sourceSchemaMirror("reserved_renamed", "reserved_renamed")
		renamed.Resync = resync
		renamed.TableMappings[0].Columns = []*protos.ColumnSetting{{SourceName: "name", DestinationName: "_peerdb_source_schema"}}
		s.requireValidationError(renamed, "cannot be renamed to _peerdb_source_schema")
	}

	// without the setting the column is an ordinary one
	plain := s.sourceSchemaMirror("reserved_setting_off", "reserved_on_table")
	plain.Env = map[string]string{}
	_, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: plain})
	require.NoError(s.t, err)
}

func (s APITestSuite) TestValidateCDCMirror_ChildSchemaInheritanceShape() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
	exec := func(format string, args ...any) {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(format, args...)))
	}

	deep := AttachSchema(s, "shape_deep")
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, deep)
	exec(`CREATE TABLE %s_child () INHERITS (%s)`, deep, deep)
	exec(`CREATE TABLE %s_grandchild () INHERITS (%s_child)`, deep, deep)

	p1, p2 := AttachSchema(s, "shape_p1"), AttachSchema(s, "shape_p2")
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, p1)
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, p2)
	exec(`CREATE TABLE %s_shared () INHERITS (%s, %s)`, p1, p1, p2)

	s.requireValidationError(s.sourceSchemaMirror("shape_deep", "shape_deep"), "has descendants more than one level deep")
	s.requireValidationError(s.sourceSchemaMirror("shape_multi", "shape_p1", "shape_p2"),
		"has a child that also inherits from another mirrored table")

	// mirrors that keep stamping the parent's schema accept these shapes, as before
	oldVersion := strconv.FormatUint(uint64(shared.InternalVersion_SourceSchemaFromInheritanceChild-1), 10)
	for _, cfg := range []*protos.FlowConnectionConfigs{
		s.sourceSchemaMirror("shape_deep_old", "shape_deep"),
		s.sourceSchemaMirror("shape_multi_old", "shape_p1", "shape_p2"),
	} {
		cfg.Env["PEERDB_FORCE_INTERNAL_VERSION"] = oldVersion
		_, err := s.ValidateCDCMirror(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: cfg})
		require.NoError(s.t, err)
	}
}
