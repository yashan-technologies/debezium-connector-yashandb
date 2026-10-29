/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.yashandb.ystream.YStreamOffsetContextLoader;

class YashanDbRecoveryPositionTest {

    @Test
    void shouldKeepKafkaPositionByDefaultAfterLoadingOffset() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "recovery_test").build());
        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(storedOffset());
        YashanDbConnection connection = mock(YashanDbConnection.class);

        YashanDbConnectorTask.applyRecoveryPosition(config, offset, connection);

        storedOffset().forEach((key, value) -> assertThat(offset.getOffset().get(key)).isEqualTo(value));
        verifyNoInteractions(connection);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldOverrideKafkaPositionWithoutLosingSnapshotState(boolean snapshotRunning) {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(configuredPosition().build());
        Map<String, Object> stored = storedOffset();
        if (snapshotRunning) {
            stored.put(SourceInfo.SNAPSHOT_KEY, SnapshotType.INITIAL.toString());
            stored.put(YashanDbOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
        }
        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(stored);
        YashanDbConnection connection = mock(YashanDbConnection.class);

        // Startup must apply the configuration even when no snapshot is executed.
        YashanDbConnectorTask.applyRecoveryPosition(config, offset, connection);

        assertThat(offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(123456L);
        assertThat(offset.getRecoverPosition().getLogPosition().getInstanceId()).isEqualTo((byte) -128);
        assertThat(offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(200L);
        assertThat(offset.getRecoverPosition().getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(offset.getRecoverPosition().getLogPosition().getBatchRowId()).isEqualTo(7);
        assertThat(offset.getSnapshotScn()).isEqualTo(Scn.valueOf(100));
        assertThat(offset.getSnapshotPendingTransactions()).containsEntry("tx1", Scn.valueOf(90));
        assertThat(offset.isInitialSnapshotRunning()).isEqualTo(snapshotRunning);
        verifyNoInteractions(connection);
    }

    @Test
    void shouldUseNextCurrentScnWhenOnlyLogPositionIsConfigured() throws Exception {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "recovery_test")
                .with("snapshot.mode", "no_data")
                .with("streaming.group.lsn", 17L).build());
        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(storedOffset());
        YashanDbConnection connection = mock(YashanDbConnection.class);
        when(connection.getCurrentScn()).thenReturn(Scn.valueOf(500));

        YashanDbConnectorTask.applyRecoveryPosition(config, offset, connection);

        assertThat(offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(501L);
        assertThat(offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(17L);
        assertThat(offset.getRecoverPosition().getLogPosition().getInstanceId()).isZero();
        assertThat(offset.getSnapshotScn()).isEqualTo(Scn.valueOf(100));
    }

    @Test
    void shouldApplyExplicitZeroPositionComponents() throws Exception {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "recovery_test")
                .with("snapshot.mode", "recovery")
                .with("streaming.group.offset", 0).build());
        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(storedOffset());
        YashanDbConnection connection = mock(YashanDbConnection.class);
        when(connection.getCurrentScn()).thenReturn(Scn.valueOf(500));

        YashanDbConnectorTask.applyRecoveryPosition(config, offset, connection);

        assertThat(offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(501L);
        assertThat(offset.getRecoverPosition().getLogPosition().getGroupOffset()).isZero();
    }

    @Test
    void shouldUseMaximumStreamingScnWithoutIncrementingIt() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(configuredPosition()
                .with("streaming.scn", Long.MAX_VALUE).build());
        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(storedOffset());
        YashanDbConnection connection = mock(YashanDbConnection.class);

        YashanDbConnectorTask.applyRecoveryPosition(config, offset, connection);

        assertThat(offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(Long.MAX_VALUE);
        verifyNoInteractions(connection);
    }

    @Test
    void shouldLeaveFreshStartupToDetermineItsSnapshotOffset() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(configuredPosition().build());
        YashanDbConnection connection = mock(YashanDbConnection.class);

        YashanDbConnectorTask.applyRecoveryPosition(config, null, connection);

        verifyNoInteractions(connection);
    }

    @Test
    void shouldLoadCommittedKafkaPositionWithoutApplyingStartupOverrideAgain() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(configuredPosition().build());

        YashanDbOffsetContext offset = new YStreamOffsetContextLoader(config).load(storedOffset());

        storedOffset().forEach((key, value) -> assertThat(offset.getOffset().get(key)).isEqualTo(value));
    }

    private static Configuration.Builder configuredPosition() {
        return Configuration.create()
                .with("topic.prefix", "recovery_test")
                .with("snapshot.mode", "no_data")
                .with("streaming.scn", "123456")
                .with("streaming.instance.id", -128)
                .with("streaming.group.lsn", 200L)
                .with("streaming.group.offset", 15)
                .with("streaming.batch.row.id", 7);
    }

    private static Map<String, Object> storedOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "100");
        offset.put(YashanDbOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY, "tx1:90");
        offset.put(SourceInfo.POSITION_SCN_KEY, 300L);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "1");
        offset.put(SourceInfo.GROUP_LSN_KEY, 400L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 20);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 8);
        return offset;
    }
}
