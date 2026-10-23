/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbModeContext;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

class YashanDbDialectKeyMetadataTest {

    private final TableId table = new TableId(null, "APP", "ORDERS");
    private final YashanDbDialect dialect = new YashanDbMySqlDialect();
    private YashanDbConnection connection;
    private PreparedStatement statement;

    @BeforeEach
    void setUp() throws SQLException {
        Connection jdbcConnection = mock(Connection.class);
        statement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(statement);
        connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return jdbcConnection;
            }
        };
    }

    @Test
    void shouldSkipEntireFilteredIndexAndReturnFirstIncludedCompositeIndex() throws SQLException {
        ResultSet rows = rows(new Object[]{ "A_SKIP", "FIRST", 1 }, new Object[]{ "A_SKIP", "SECRET", 2 },
                new Object[]{ "B_ACCEPT", "REGION", 1 }, new Object[]{ "B_ACCEPT", "ID", 2 },
                new Object[]{ "C_OTHER", "OTHER", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        List<String> columns = dialect.readTableUniqueIndices(connection, table,
                (index, column) -> !"SECRET".equals(column), () -> {
                    throw new AssertionError("MySQL metadata must use SQL");
                });

        assertThat(columns).containsExactly("REGION", "ID");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
        verify(rows).close();
    }

    @Test
    void shouldReturnNoColumnsWhenAllUniqueIndicesAreFiltered() throws SQLException {
        ResultSet rows = rows(new Object[]{ "A_SKIP", "ID", 1 }, new Object[]{ "B_SKIP", "REGION", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        assertThat(dialect.readTableUniqueIndices(connection, table, (index, column) -> false, () -> {
            throw new AssertionError("MySQL metadata must use SQL");
        })).isEmpty();
    }

    @Test
    void shouldReadCompositePrimaryKeyInQueryOrder() throws SQLException {
        ResultSet rows = rows(new Object[]{ "REGION" }, new Object[]{ "ID" });
        when(statement.executeQuery()).thenReturn(rows);

        assertThat(dialect.readPrimaryKeyNames(connection, table, () -> {
            throw new AssertionError("MySQL metadata must use SQL");
        })).containsExactly("REGION", "ID");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
    }

    @Test
    void shouldUseJdbcReadersForNativeKeys() throws SQLException {
        YashanDbDialect nativeDialect = new YashanDbNativeDialect();

        assertThat(nativeDialect.readPrimaryKeyNames(connection, table, () -> List.of("REGION", "ID")))
                .containsExactly("REGION", "ID");
        assertThat(nativeDialect.readTableUniqueIndices(connection, table, (index, column) -> true, () -> List.of("ID")))
                .containsExactly("ID");
    }

    private static ResultSet rows(Object[]... values) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger rowIndex = new AtomicInteger(-1);
        when(result.next()).thenAnswer(invocation -> rowIndex.incrementAndGet() < values.length);
        when(result.getString(anyInt())).thenAnswer(invocation -> values[rowIndex.get()][invocation.<Integer> getArgument(0) - 1]);
        when(result.getInt(anyInt())).thenAnswer(invocation -> values[rowIndex.get()][invocation.<Integer> getArgument(0) - 1]);
        return result;
    }
}
