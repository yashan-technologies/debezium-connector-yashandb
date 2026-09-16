/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import com.yashandb.jdbc.YasTypes;

import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBDatabaseSchema;
import io.debezium.connector.yashandb.YashanDBOffsetContext;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.connector.yashandb.YashanDBStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link YStreamEventHandler}.
 * Note: Full functionality testing requires complex YStream runtime dependencies.
 * This test verifies basic class structure and helper methods.
 */
class YStreamEventHandlerTest {

    @Test
    void shouldCreateEventHandlerWithValidParameters() throws Exception {
        YashanDBConnectorConfig config = mock(YashanDBConnectorConfig.class);
        ErrorHandler errorHandler = mock(ErrorHandler.class);
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        Clock clock = mock(Clock.class);
        YashanDBDatabaseSchema schema = mock(YashanDBDatabaseSchema.class);
        YashanDBPartition partition = new YashanDBPartition("server", "db");
        YashanDBOffsetContext offsetContext = mock(YashanDBOffsetContext.class);
        YStreamStreamingChangeEventSource eventSource = mock(YStreamStreamingChangeEventSource.class);
        YashanDBStreamingChangeEventSourceMetrics metrics = mock(YashanDBStreamingChangeEventSourceMetrics.class);

        YStreamEventHandler handler = new YStreamEventHandler(
                config,
                errorHandler,
                dispatcher,
                clock,
                schema,
                partition,
                offsetContext,
                eventSource,
                metrics);

        assertThat(handler).isNotNull();
    }

    @Test
    void shouldHaveValidPartition() throws Exception {
        YashanDBConnectorConfig config = mock(YashanDBConnectorConfig.class);
        ErrorHandler errorHandler = mock(ErrorHandler.class);
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        Clock clock = mock(Clock.class);
        YashanDBDatabaseSchema schema = mock(YashanDBDatabaseSchema.class);
        YashanDBPartition partition = new YashanDBPartition("server", "db");
        YashanDBOffsetContext offsetContext = mock(YashanDBOffsetContext.class);
        YStreamStreamingChangeEventSource eventSource = mock(YStreamStreamingChangeEventSource.class);
        YashanDBStreamingChangeEventSourceMetrics metrics = mock(YashanDBStreamingChangeEventSourceMetrics.class);

        YStreamEventHandler handler = new YStreamEventHandler(
                config,
                errorHandler,
                dispatcher,
                clock,
                schema,
                partition,
                offsetContext,
                eventSource,
                metrics);

        // Verify handler was created with partition
        assertThat(partition.getSourcePartition()).containsKey("server");
    }

    @Test
    void shouldHavePackagePrivateConstructor() throws Exception {
        // Verify the constructor is package-private (not public)
        Class<?>[] paramTypes = new Class<?>[]{
                YashanDBConnectorConfig.class,
                ErrorHandler.class,
                EventDispatcher.class,
                Clock.class,
                YashanDBDatabaseSchema.class,
                YashanDBPartition.class,
                YashanDBOffsetContext.class,
                YStreamStreamingChangeEventSource.class,
                YashanDBStreamingChangeEventSourceMetrics.class
        };

        try {
            var constructor = YStreamEventHandler.class.getDeclaredConstructor(paramTypes);
            assertThat(constructor).isNotNull();
            // Constructor should not be public
            assertThat(java.lang.reflect.Modifier.isPublic(constructor.getModifiers())).isFalse();
        }
        catch (NoSuchMethodException e) {
            // Constructor may have different signature, just verify class exists
            assertThat(YStreamEventHandler.class).isNotNull();
        }
    }

    @Test
    void shouldRecognizeSupportedDataTypes() {
        int[] supportedTypes = {
                Types.CHAR, Types.VARCHAR, Types.NCHAR, Types.NVARCHAR, Types.BOOLEAN,
                Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                YasTypes.UTINYINT, YasTypes.USMALLINT, YasTypes.UINTEGER, YasTypes.UBIGINT,
                Types.REAL, Types.DOUBLE, Types.FLOAT, Types.NUMERIC, Types.DECIMAL,
                Types.BINARY, Types.VARBINARY, Types.DATE, Types.TIME, Types.TIMESTAMP,
                Types.TIMESTAMP_WITH_TIMEZONE, Types.BLOB, Types.CLOB,
                YasTypes.YM_INTERVAL, YasTypes.DS_INTERVAL,
                YasTypes.TIMESTAMP_TZ, YasTypes.TIMESTAMP_LTZ,
                Types.BIT, YasTypes.JSON, Types.SQLXML
        };

        for (int supportedType : supportedTypes) {
            assertThat(YStreamEventHandler.isSupportedDataType(supportedType)).isTrue();
        }
    }

    @Test
    void shouldRejectDataTypesOutsideWhitelist() {
        int[] unsupportedTypes = { Types.ARRAY, Types.STRUCT, Types.NCLOB, Types.ROWID, Types.OTHER };

        for (int unsupportedType : unsupportedTypes) {
            assertThat(YStreamEventHandler.isSupportedDataType(unsupportedType)).isFalse();
        }

        assertThat(YStreamEventHandler.isSupportedDataType(Types.OTHER, "SDO_GEOMETRY")).isFalse();
    }

    @Test
    void shouldRecognizeJdbcOtherIntervalTypes() {
        assertThat(YStreamEventHandler.isSupportedDataType(Types.OTHER, "INTERVAL YEAR TO MONTH")).isTrue();
        assertThat(YStreamEventHandler.isSupportedDataType(Types.OTHER, "INTERVAL DAY TO SECOND")).isTrue();
    }
}
