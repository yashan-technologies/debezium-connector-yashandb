/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

class YashanDbModeDetectorTest {

    private Connection connection;
    private ResultSet result;

    @BeforeEach
    void setUp() throws SQLException {
        connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        result = mock(ResultSet.class);
        when(connection.prepareStatement(YashanDbModeDetector.QUERY)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
    }

    @Test
    void shouldDetectMysqlMode() throws SQLException {
        when(result.next()).thenReturn(true, false);
        when(result.getString(1)).thenReturn(" mysql ");
        assertThat(YashanDbModeDetector.detect(connection).isMySql()).isTrue();
    }

    @Test
    void shouldDetectNativeMode() throws SQLException {
        when(result.next()).thenReturn(true, false);
        when(result.getString(1)).thenReturn("YASHAN");
        assertThat(YashanDbModeDetector.detect(connection).isMySql()).isFalse();
    }

    @Test
    void shouldRejectMissingValue() throws SQLException {
        when(result.next()).thenReturn(false);
        assertThatThrownBy(() -> YashanDbModeDetector.detect(connection))
                .isInstanceOf(DebeziumException.class).hasMessageContaining("no rows");
    }

    @Test
    void shouldRejectMultipleRows() throws SQLException {
        when(result.next()).thenReturn(true, true);
        when(result.getString(1)).thenReturn("MYSQL");
        assertThatThrownBy(() -> YashanDbModeDetector.detect(connection))
                .isInstanceOf(DebeziumException.class).hasMessageContaining("multiple rows");
    }

    @Test
    void shouldRejectNullValue() throws SQLException {
        when(result.next()).thenReturn(true, false);
        assertThatThrownBy(() -> YashanDbModeDetector.detect(connection))
                .isInstanceOf(DebeziumException.class).hasMessageContaining("null");
    }

    @Test
    void shouldRejectUnknownValue() throws SQLException {
        when(result.next()).thenReturn(true, false);
        when(result.getString(1)).thenReturn("OTHER");
        assertThatThrownBy(() -> YashanDbModeDetector.detect(connection))
                .isInstanceOf(DebeziumException.class).hasMessageContaining("Unsupported COMPAT_VECTOR");
    }

    @Test
    void shouldReportSqlFailure() throws SQLException {
        when(connection.prepareStatement(YashanDbModeDetector.QUERY)).thenThrow(new SQLException("permission denied"));
        assertThatThrownBy(() -> YashanDbModeDetector.detect(connection))
                .isInstanceOf(DebeziumException.class).hasMessageContaining("Unable to determine database mode")
                .hasCauseInstanceOf(SQLException.class);
    }
}
