/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.connector.yashandb.ystream.YStreamOffsetContextLoader;
import io.debezium.connector.yashandb.ystream.YStreamPosition;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;

class YashanDbSnapshotScnTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldUseExactStreamingScnAndDetermineSnapshotScnAutomatically(boolean mysqlMode) throws Exception {
        SnapshotFixture fixture = new SnapshotFixture("882374440345255936", mysqlMode);

        fixture.source.determineSnapshotOffset(fixture.context, null);
        fixture.context.offset.preSnapshotStart(false);

        assertThat(fixture.context.offset.getSnapshotScn()).isEqualTo(Scn.valueOf(200));
        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(882374440345255936L);
        assertThat(fixture.query.snapshotQuery("APP.ORDERS", List.of("ID", "NAME")))
                .contains("SELECT ID, NAME FROM APP.ORDERS AS OF SCN 200");

        YashanDbOffsetContext restored = new YStreamOffsetContextLoader(fixture.config).load(fixture.context.offset.getOffset());
        assertThat(restored.getSnapshotQueryScn()).isEqualTo(Scn.valueOf(200));
        assertThat(restored.getRecoverPosition().getCommitScn().getScn()).isEqualTo(882374440345255936L);
    }

    @Test
    void shouldUseConfiguredYStreamLogPositionWithoutDataSnapshot() throws Exception {
        SnapshotFixture fixture = new SnapshotFixture("123456", false, -128, 200L, 15, 7);

        fixture.source.determineSnapshotOffset(fixture.context, null);

        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(123456L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getInstanceId()).isEqualTo((byte) -128);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(200L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getBatchRowId()).isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "always", "initial_only", "no_data", "recovery" })
    void shouldRetainAutomaticSnapshotAndNextScnStreamingByDefault(String snapshotMode) throws Exception {
        SnapshotFixture fixture = new SnapshotFixture(null, false, null, null, null, null, snapshotMode);

        fixture.source.determineSnapshotOffset(fixture.context, null);
        fixture.context.offset.preSnapshotStart(false);

        assertThat(fixture.context.offset.getSnapshotQueryScn()).isEqualTo(Scn.valueOf(200));
        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(201L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getInstanceId()).isEqualTo((byte) 0);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(0L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupOffset()).isEqualTo(0);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getBatchRowId()).isEqualTo(0);
        assertThat(fixture.query.snapshotQuery("APP.ORDERS", List.of("ID")))
                .contains("SELECT ID FROM APP.ORDERS AS OF SCN 200");
    }

    @Test
    void shouldUseAutomaticScnWithConfiguredLogPositionOnFreshStartup() throws Exception {
        SnapshotFixture fixture = new SnapshotFixture(null, false, -128, 200L, 15, 7, "no_data");

        fixture.source.determineSnapshotOffset(fixture.context, null);

        assertThat(fixture.context.offset.getSnapshotScn()).isEqualTo(Scn.valueOf(200));
        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(201L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getInstanceId()).isEqualTo((byte) -128);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(200L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getBatchRowId()).isEqualTo(7);
    }

    @Test
    void shouldPreserveSavedOffsetWhenSnapshotterResumesStreamingFromIt() throws Exception {
        SnapshotFixture fixture = new SnapshotFixture(null, false);
        when(fixture.snapshotter.shouldStreamEventsStartingFromSnapshot()).thenReturn(false);
        YashanDbOffsetContext previousOffset = YashanDbOffsetContext.create()
                .logicalName(fixture.config)
                .snapshotScn(Scn.valueOf(100))
                .recoverPosition(YStreamPosition.valueOf(200).getLcrPosition())
                .snapshotPendingTransactions(Collections.emptyMap())
                .build();

        fixture.source.determineSnapshotOffset(fixture.context, previousOffset);

        assertThat(fixture.context.offset).isSameAs(previousOffset);
        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(200L);
        verify(fixture.connection, never()).getCurrentScn();
    }

    @Test
    void shouldReuseConfiguredPositionAppliedAfterLoadingKafkaOffset() throws Exception {
        SnapshotFixture fixture = new SnapshotFixture("123456", false, -128, 200L, 15, 7, "recovery");
        when(fixture.snapshotter.shouldStreamEventsStartingFromSnapshot()).thenReturn(false);
        YashanDbOffsetContext previousOffset = YashanDbOffsetContext.create()
                .logicalName(fixture.config)
                .snapshotScn(Scn.valueOf(100))
                .recoverPosition(YStreamPosition.valueOf(300).getLcrPosition())
                .snapshotPendingTransactions(Collections.emptyMap())
                .build();

        YashanDbConnectorTask.applyRecoveryPosition(fixture.config, previousOffset, fixture.connection);
        fixture.source.determineSnapshotOffset(fixture.context, previousOffset);

        assertThat(fixture.context.offset).isSameAs(previousOffset);
        assertThat(fixture.context.offset.getSnapshotScn()).isEqualTo(Scn.valueOf(100));
        assertThat(fixture.context.offset.getRecoverPosition().getCommitScn().getScn()).isEqualTo(123456L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getInstanceId()).isEqualTo((byte) -128);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupLsn()).isEqualTo(200L);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(fixture.context.offset.getRecoverPosition().getLogPosition().getBatchRowId()).isEqualTo(7);
    }

    @Test
    void shouldResumeCompletedSnapshotFromSavedStreamingPosition() throws Exception {
        SnapshotFixture fixture = new SnapshotFixture(null, false);
        fixture.source.determineSnapshotOffset(fixture.context, null);
        YashanDbOffsetContext offset = fixture.context.offset;
        offset.preSnapshotStart(false);
        offset.preSnapshotCompletion();
        offset.postSnapshotCompletion();
        offset.setLcrPosition(YStreamPosition.valueOf(234567).getLcrPosition());

        YashanDbOffsetContext restored = new YStreamOffsetContextLoader(fixture.config).load(offset.getOffset());

        assertThat(restored.getSnapshotScn()).isEqualTo(Scn.valueOf(200));
        assertThat(restored.getRecoverPosition().getCommitScn().getScn()).isEqualTo(234567L);
        assertThat(restored.isInitialSnapshotRunning()).isFalse();
    }

    private static class SnapshotFixture {
        final YashanDbConnection connection = mock(YashanDbConnection.class);
        final Snapshotter snapshotter = mock(Snapshotter.class);
        final SelectAllSnapshotQuery query = new SelectAllSnapshotQuery();
        final YashanDbConnectorConfig config;
        final YashanDbSnapshotChangeEventSource source;
        final RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> context;

        SnapshotFixture(String scn, boolean mysqlMode) throws SQLException {
            this(scn, mysqlMode, null, null, null, null, scn == null ? "initial" : "no_data");
        }

        SnapshotFixture(String scn, boolean mysqlMode, Integer instanceId, Long groupLsn, Integer groupOffset, Integer batchRowId) throws SQLException {
            this(scn, mysqlMode, instanceId, groupLsn, groupOffset, batchRowId, "no_data");
        }

        SnapshotFixture(String scn, boolean mysqlMode, Integer instanceId, Long groupLsn, Integer groupOffset, Integer batchRowId,
                        String snapshotMode)
                throws SQLException {
            Configuration.Builder builder = Configuration.create()
                    .with("topic.prefix", "snapshot_test")
                    .with("snapshot.mode", snapshotMode)
                    .with("ystream.additional.properties.mysql.mode", mysqlMode);
            if (scn != null) {
                builder.with("streaming.scn", scn);
            }
            if (instanceId != null) {
                builder.with("streaming.instance.id", instanceId);
            }
            if (groupLsn != null) {
                builder.with("streaming.group.lsn", groupLsn);
            }
            if (groupOffset != null) {
                builder.with("streaming.group.offset", groupOffset);
            }
            if (batchRowId != null) {
                builder.with("streaming.batch.row.id", batchRowId);
            }
            config = new YashanDbConnectorConfig(builder.build());
            when(connection.getCurrentScn()).thenReturn(Scn.valueOf(200));
            MainConnectionProvidingConnectionFactory<YashanDbConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
            when(factory.mainConnection()).thenReturn(connection);
            SnapshotterService snapshotterService = mock(SnapshotterService.class);
            when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
            source = new YashanDbSnapshotChangeEventSource(config, factory, mock(YashanDbDatabaseSchema.class), null, null, null, null, snapshotterService);
            context = (RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext>) source.prepare(new YashanDbPartition("server", "database"), false);
            context.capturedTables = Collections.emptySet();
            DefaultBeanRegistry registry = new DefaultBeanRegistry();
            registry.add(StandardBeanNames.SNAPSHOT_CONTEXT, context);
            query.injectBeanRegistry(registry);
        }
    }
}
