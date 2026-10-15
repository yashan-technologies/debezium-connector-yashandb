/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class YashanDbConnectionSchemaTest {

    private DatabaseMetaData metadata;
    private Connection jdbcConnection;
    private YashanDbConnection connection;
    private Tables tables;

    @BeforeEach
    void setUp() throws SQLException {
        metadata = mock(DatabaseMetaData.class);
        jdbcConnection = mock(Connection.class);
        when(jdbcConnection.getMetaData()).thenReturn(metadata);
        when(metadata.getSearchStringEscape()).thenReturn("\\");
        ResultSet emptyResult = resultSet();
        // Reproduce the driver's failure when only a schema is supplied.
        when(metadata.getColumns(any(), any(), isNull(), any()))
                .thenThrow(new SQLSyntaxErrorException("YAS-04323 arguments count must be 2"));
        doReturn(emptyResult).when(metadata).getPrimaryKeys(any(), any(), any());
        doReturn(emptyResult).when(metadata).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
        connection = new YashanDbConnection(JdbcConfiguration.create().build(), config -> jdbcConnection);
        tables = new Tables();
    }

    @Test
    void shouldReadAllTablesUsingSchemaAndTableNames() throws SQLException {
        ResultSet tableMetadata = tableMetadata("APP", "ORDERS", "TABLE", "APP", "CUSTOMERS", "TABLE");
        when(metadata.getTables(any(), any(), any(), any())).thenReturn(tableMetadata);
        ResultSet orderColumns = resultSet(column("APP", "ORDERS", "DESCRIPTION", Types.VARCHAR, 2),
                column("APP", "ORDERS", "ID", Types.NUMERIC, 1));
        when(metadata.getColumns(null, "APP", "ORDERS", null)).thenReturn(orderColumns);
        doReturn(resultSet(column("APP", "CUSTOMERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP", "CUSTOMERS", null);
        ResultSet primaryKeys = resultSet(new Object[]{ null, "APP", "ORDERS", "DESCRIPTION", 2 },
                new Object[]{ null, "APP", "ORDERS", "ID", 1 });
        when(metadata.getPrimaryKeys("", "APP", "ORDERS")).thenReturn(primaryKeys);

        connection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(tableId("ORDERS"), tableId("CUSTOMERS"));
        Table orders = tables.forTable(tableId("ORDERS"));
        assertThat(orders.columns()).extracting(Column::name).containsExactly("ID", "DESCRIPTION");
        assertThat(orders.primaryKeyColumnNames()).containsExactly("ID", "DESCRIPTION");
        assertThat(orders.columnWithName("ID").scale()).isEmpty();
        assertThat(orders.columnWithName("ID").defaultValueExpression()).contains("0");
        assertThat(orders.columnWithName("DESCRIPTION").isOptional()).isTrue();
        verify(metadata, never()).getColumns(any(), any(), isNull(), any());
        verify(tableMetadata).close();
        verify(orderColumns).close();
        verify(primaryKeys).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldApplyFiltersAndExcludeViewsEvenWhenFullColumnScanIsForced(boolean forceFullColumnScan) throws SQLException {
        connection = new YashanDbConnection(JdbcConfiguration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_FULL_COLUMN_SCAN_FORCE.name(), Boolean.toString(forceFullColumnScan))
                .build(), config -> jdbcConnection);
        doReturn(tableMetadata("APP", "ORDERS", "TABLE", "APP", "EXCLUDED", "TABLE",
                "APP", "ORDER_VIEW", "VIEW", "APP", "ORDER_MV", "MATERIALIZED VIEW"))
                .when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP", "ORDERS", "ID", Types.INTEGER, 1),
                column("APP", "ORDERS", "SECRET", Types.VARCHAR, 2)))
                .when(metadata).getColumns(null, "APP", "ORDERS", null);

        connection.readSchema(tables, null, "APP", Tables.TableFilter.fromPredicate(tableId("ORDERS")::equals),
                (catalog, schema, table, column) -> !column.equals("SECRET"), false);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
        assertThat(tables.forTable(tableId("ORDERS")).columns()).extracting(Column::name).containsExactly("ID");
        verify(metadata, never()).getColumns(null, "APP", "EXCLUDED", null);
        verify(metadata, never()).getColumns(null, "APP", "ORDER_VIEW", null);
        verify(metadata, never()).getColumns(null, "APP", "ORDER_MV", null);
        verify(metadata, never()).getColumns(any(), any(), isNull(), any());
    }

    @Test
    void shouldEscapeSchemaAndTableNamesForColumnMetadata() throws SQLException {
        doReturn(tableMetadata("APP_SCHEMA", "ORDER_%", "TABLE")).when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP_SCHEMA", "ORDER_%", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP\\_SCHEMA", "ORDER\\_\\%", null);

        connection.readSchema(tables, null, "APP_SCHEMA", null, null, false);

        assertThat(tables.tableIds()).containsExactly(new TableId("", "APP_SCHEMA", "ORDER_%"));
        verify(metadata).getTables(isNull(), eq("APP\\_SCHEMA"), isNull(), any(String[].class));
        verify(metadata).getColumns(null, "APP\\_SCHEMA", "ORDER\\_\\%", null);
    }

    @Test
    void shouldReadColumnsFromEachSchemaWhenSchemaIsNotSpecified() throws SQLException {
        doReturn(tableMetadata("APP", "ORDERS", "TABLE", "OTHER", "ORDERS", "TABLE"))
                .when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP", "ORDERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP", "ORDERS", null);
        doReturn(resultSet(column("OTHER", "ORDERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "OTHER", "ORDERS", null);

        connection.readSchema(tables, null, null, null, null, false);

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(tableId("ORDERS"), new TableId("", "OTHER", "ORDERS"));
    }

    @Test
    void shouldUseCatalogForMysqlModeMetadata() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        doReturn(resultSet(new Object[]{ "APP", null, "ORDERS", "TABLE" }))
                .when(metadata).getTables(eq("APP"), isNull(), isNull(), any(String[].class));
        Object[] orderId = column(null, "ORDERS", "ID", Types.INTEGER, 1);
        orderId[0] = "APP";
        doReturn(resultSet(orderId)).when(metadata).getColumns("APP", null, "ORDERS", null);

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        TableId orders = new TableId("APP", null, "ORDERS");
        assertThat(mysqlConnection.createTableId("APP", null, "ORDERS")).isEqualTo(orders);
        assertThat(tables.tableIds()).containsExactly(orders);
        verify(metadata).getTables(eq("APP"), isNull(), isNull(), any(String[].class));
        verify(metadata).getColumns("APP", null, "ORDERS", null);
        verify(metadata).getPrimaryKeys("APP", null, "ORDERS");
        verify(metadata).getIndexInfo("APP", null, "ORDERS", true, true);
        verify(metadata, never()).getTables(isNull(), eq("APP"), isNull(), any(String[].class));
    }

    @Test
    void shouldUseUniqueIndexWhenTableHasNoPrimaryKey() throws SQLException {
        doReturn(tableMetadata("APP", "ORDERS", "TABLE")).when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP", "ORDERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP", "ORDERS", null);
        doReturn(resultSet(new Object[]{ null, "APP", "ORDERS", false, null, "ORDERS_UK", 3, 1, "ID" }))
                .when(metadata).getIndexInfo("", "APP", "ORDERS", true, true);

        connection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.forTable(tableId("ORDERS")).primaryKeyColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldHonorRemovalFlagWhenNoTablesAreFound(boolean removeTablesNotFoundInJdbc) throws SQLException {
        tables.overwriteTable(tableId("DROPPED"), List.of(Column.editor().name("ID").position(1).create()), List.of("ID"), null, List.of());
        doReturn(resultSet()).when(metadata).getTables(any(), any(), any(), any());

        connection.readSchema(tables, null, "APP", null, null, removeTablesNotFoundInJdbc);

        assertThat(tables.forTable(tableId("DROPPED")) == null).isEqualTo(removeTablesNotFoundInJdbc);
        verify(metadata, never()).getColumns(any(), any(), any(), any());
    }

    @Test
    void shouldRetainTableDefinitionWhenAllColumnsAreExcluded() throws SQLException {
        doReturn(tableMetadata("APP", "ORDERS", "TABLE")).when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP", "ORDERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP", "ORDERS", null);

        connection.readSchema(tables, null, "APP", null, (catalog, schema, table, column) -> false, true);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
        assertThat(tables.forTable(tableId("ORDERS")).columns()).isEmpty();
    }

    @Test
    void shouldRetainOnlyTablesWithIncludedColumnsWhenSomeTablesAreFullyExcluded() throws SQLException {
        doReturn(tableMetadata("APP", "ORDERS", "TABLE", "APP", "AUDIT", "TABLE"))
                .when(metadata).getTables(any(), any(), any(), any());
        doReturn(resultSet(column("APP", "ORDERS", "ID", Types.INTEGER, 1)))
                .when(metadata).getColumns(null, "APP", "ORDERS", null);
        doReturn(resultSet(column("APP", "AUDIT", "SECRET", Types.VARCHAR, 1)))
                .when(metadata).getColumns(null, "APP", "AUDIT", null);

        connection.readSchema(tables, null, "APP", null, (catalog, schema, table, column) -> !column.equals("SECRET"), false);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
    }

    private static TableId tableId(String table) {
        return new TableId("", "APP", table);
    }

    private static ResultSet tableMetadata(String... schemaTableTypes) throws SQLException {
        Object[][] rows = new Object[schemaTableTypes.length / 3][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{ null, schemaTableTypes[i * 3], schemaTableTypes[i * 3 + 1], schemaTableTypes[i * 3 + 2] };
        }
        return resultSet(rows);
    }

    private static Object[] column(String schema, String table, String name, int jdbcType, int position) {
        Object[] row = new Object[24];
        row[1] = schema;
        row[2] = table;
        row[3] = name;
        row[4] = jdbcType;
        row[5] = jdbcType == Types.NUMERIC ? "NUMBER" : "VARCHAR";
        row[6] = 100;
        row[8] = jdbcType == Types.NUMERIC ? -127 : null;
        row[10] = DatabaseMetaData.columnNullable;
        row[12] = "0";
        row[16] = position;
        return row;
    }

    private static ResultSet resultSet(Object[]... rows) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger index = new AtomicInteger(-1);
        when(result.next()).thenAnswer(invocation -> index.incrementAndGet() < rows.length);
        when(result.getString(anyInt())).thenAnswer(invocation -> {
            Object value = rows[index.get()][invocation.<Integer> getArgument(0) - 1];
            return value == null ? null : value.toString();
        });
        when(result.getInt(anyInt())).thenAnswer(invocation -> {
            Object value = rows[index.get()][invocation.<Integer> getArgument(0) - 1];
            return value == null ? 0 : ((Number) value).intValue();
        });
        when(result.getObject(anyInt())).thenAnswer(invocation -> rows[index.get()][invocation.<Integer> getArgument(0) - 1]);
        return result;
    }
}
