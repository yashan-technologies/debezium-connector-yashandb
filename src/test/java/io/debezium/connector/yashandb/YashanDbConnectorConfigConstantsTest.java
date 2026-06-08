/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Field;

/**
 * Unit tests for {@link YashanDBConnectorConfig} configuration constants.
 */
class YashanDBConnectorConfigConstantsTest {

    @Test
    void shouldHaveDefaultPort() {
        assertThat(YashanDBConnectorConfig.DEFAULT_PORT).isEqualTo(1688);
    }

    @Test
    void shouldHaveDefaultQueryFetchSize() {
        assertThat(YashanDBConnectorConfig.DEFAULT_QUERY_FETCH_SIZE).isEqualTo(10_000);
    }

    @Test
    void shouldHaveExcludedSchemas() {
        List<String> excludedSchemas = YashanDBConnectorConfig.EXCLUDED_SCHEMAS;
        assertThat(excludedSchemas).isNotNull();
        assertThat(excludedSchemas).isNotEmpty();
        assertThat(excludedSchemas).contains("SYS");
        assertThat(excludedSchemas).contains("MDSYS");
        assertThat(excludedSchemas).contains("XA_SYS");
    }

    @Test
    void shouldHaveAllFields() {
        Field.Set allFields = YashanDBConnectorConfig.ALL_FIELDS;
        assertThat(allFields).isNotNull();
        assertThat(allFields).isNotEmpty();
    }

    @Test
    void shouldHavePortField() {
        assertThat(YashanDBConnectorConfig.PORT).isNotNull();
        assertThat(YashanDBConnectorConfig.PORT.name()).isEqualTo("database.port");
    }

    @Test
    void shouldHaveHostnameField() {
        assertThat(YashanDBConnectorConfig.HOSTNAME).isNotNull();
        assertThat(YashanDBConnectorConfig.HOSTNAME.name()).isEqualTo("database.hostname");
    }

    @Test
    void shouldHaveUserField() {
        assertThat(YashanDBConnectorConfig.USER).isNotNull();
    }

    @Test
    void shouldHavePasswordField() {
        assertThat(YashanDBConnectorConfig.PASSWORD).isNotNull();
    }

    @Test
    void shouldHaveDatabaseNameField() {
        assertThat(YashanDBConnectorConfig.DATABASE_NAME).isNotNull();
    }

    @Test
    void shouldHaveSnapshotModeField() {
        assertThat(YashanDBConnectorConfig.SNAPSHOT_MODE).isNotNull();
        assertThat(YashanDBConnectorConfig.SNAPSHOT_MODE.name()).isEqualTo("snapshot.mode");
    }

    @Test
    void shouldHaveUrlField() {
        assertThat(YashanDBConnectorConfig.URL).isNotNull();
        assertThat(YashanDBConnectorConfig.URL.name()).isEqualTo("database.url");
    }

    @Test
    void shouldHaveYstreamServerNameField() {
        assertThat(YashanDBConnectorConfig.YSTREAM_SERVER_NAME).isNotNull();
        assertThat(YashanDBConnectorConfig.YSTREAM_SERVER_NAME.name()).isEqualTo("database.ystream.server.name");
    }

    @Test
    void shouldHaveLobEnabledField() {
        assertThat(YashanDBConnectorConfig.LOB_ENABLED).isNotNull();
        assertThat(YashanDBConnectorConfig.LOB_ENABLED.name()).isEqualTo("lob.enabled");
    }

    @Test
    void shouldHaveIntervalHandlingModeField() {
        assertThat(YashanDBConnectorConfig.INTERVAL_HANDLING_MODE).isNotNull();
        assertThat(YashanDBConnectorConfig.INTERVAL_HANDLING_MODE.name()).isEqualTo("interval.handling.mode");
    }

    @Test
    void shouldHaveSnapshotLockingModeField() {
        assertThat(YashanDBConnectorConfig.SNAPSHOT_LOCKING_MODE).isNotNull();
        assertThat(YashanDBConnectorConfig.SNAPSHOT_LOCKING_MODE.name()).isEqualTo("snapshot.locking.mode");
    }

    @Test
    void shouldHaveYstreamQueueSizeField() {
        assertThat(YashanDBConnectorConfig.YSTREAM_QUEUE_SIZE).isNotNull();
        assertThat(YashanDBConnectorConfig.YSTREAM_QUEUE_SIZE.name()).isEqualTo("ystream.blocking.queue.size");
    }

    @Test
    void shouldHaveYstreamPollTimeoutField() {
        assertThat(YashanDBConnectorConfig.YSTREAM_POLL_TIMEOUT).isNotNull();
        assertThat(YashanDBConnectorConfig.YSTREAM_POLL_TIMEOUT.name()).isEqualTo("ystream.poll.timeout");
    }

    @Test
    void shouldHaveYstreamClientResponseTimeoutField() {
        assertThat(YashanDBConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT).isNotNull();
        assertThat(YashanDBConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT.name()).isEqualTo("ystream.client.response.timeout");
    }

    @Test
    void shouldHaveLogicShardEnabledField() {
        assertThat(YashanDBConnectorConfig.LOGIC_SHARD_ENABLED).isNotNull();
        assertThat(YashanDBConnectorConfig.LOGIC_SHARD_ENABLED.name()).isEqualTo("logic.shard.enabled");
    }

    @Test
    void shouldHaveTableReadThreadsField() {
        assertThat(YashanDBConnectorConfig.TABLE_READ_THREADS).isNotNull();
        assertThat(YashanDBConnectorConfig.TABLE_READ_THREADS.name()).isEqualTo("table.read.threads");
    }

    @Test
    void shouldHaveDdlParseFailRetryReadTableField() {
        assertThat(YashanDBConnectorConfig.DDL_PARSE_FAIL_RETRY_READ_TABLE).isNotNull();
        assertThat(YashanDBConnectorConfig.DDL_PARSE_FAIL_RETRY_READ_TABLE.name()).isEqualTo("ddl.parse.fail.retry.read.table");
    }

    @Test
    void shouldHaveSourceInfoStructMakerField() {
        assertThat(YashanDBConnectorConfig.SOURCE_INFO_STRUCT_MAKER).isNotNull();
    }

    @Test
    void shouldHaveQueryFetchSizeField() {
        assertThat(YashanDBConnectorConfig.QUERY_FETCH_SIZE).isNotNull();
    }

    @Test
    void shouldHaveSnapshotEnhancementTokenField() {
        assertThat(YashanDBConnectorConfig.SNAPSHOT_ENHANCEMENT_TOKEN).isNotNull();
        assertThat(YashanDBConnectorConfig.SNAPSHOT_ENHANCEMENT_TOKEN.name()).isEqualTo("snapshot.enhance.predicate.scn");
    }
}
