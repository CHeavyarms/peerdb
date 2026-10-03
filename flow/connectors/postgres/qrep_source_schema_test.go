//go:build tilt

package connpostgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Validation rejects a source column named like the column PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN adds, but
// one can appear after it ran; snapshots must not read it, or the destination would receive the column twice.
func TestSnapshotExcludesReservedSourceSchemaColumn(t *testing.T) {
	t.Parallel()
	connector, schemaName := setupDB(t, "reserved")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	_, err := connector.conn.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s.reserved (id INT PRIMARY KEY, name TEXT, %s TEXT);
		INSERT INTO %s.reserved VALUES (1, 'a', 'spoofed')`,
		common.QuoteIdentifier(schemaName), internal.SourceSchemaColumnName, common.QuoteIdentifier(schemaName)))
	require.NoError(t, err)

	for setting, columns := range map[string][]string{
		"true":  {"id", "name"},
		"false": {"id", "name", internal.SourceSchemaColumnName},
	} {
		stream := model.NewQRecordStream(1)
		_, _, err := connector.PullQRepRecords(t.Context(), shared.CatalogPool{}, nil, &protos.QRepConfig{
			FlowJobName:     "test_reserved_" + setting,
			WatermarkTable:  schemaName + ".reserved",
			WatermarkColumn: "id",
			Env:             map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": setting},
		}, protos.DBType_CLICKHOUSE, &protos.QRepPartition{PartitionId: "full", FullTablePartition: true}, stream)
		require.NoError(t, err, setting)
		schema, err := stream.Schema()
		require.NoError(t, err, setting)
		require.Equal(t, columns, schema.GetColumnNames(), setting)
	}
}
