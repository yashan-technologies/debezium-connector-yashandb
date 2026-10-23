/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

/** Existing YashanDB SQL behavior. */
public final class YashanDbNativeDialect implements YashanDbDialect {

    @Override
    public String name() {
        return "yashan";
    }

    @Override
    public String quoteCharacter() {
        return "\"";
    }

    @Override
    public TableId tableId(String database, String schema, String table) {
        return new TableId(database, schema, table);
    }

    @Override
    public String qualifiedTableName(TableId tableId) {
        return new TableId(tableId.catalog(), tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    @Override
    public AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        return new YashanDbDdlParser(true, false, includeComments, valueConverters, tableFilter);
    }

    @Override
    public String tableIdsQuery() {
        return "select owner, table_name from all_tables ";
    }

    @Override
    public List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName,
                                          JdbcMetadataReader<ResultSet> jdbcReader, UnaryOperator<String> catalogResolver)
            throws SQLException {
        List<TableMetadata> tables = new ArrayList<>();
        try (ResultSet resultSet = jdbcReader.read()) {
            while (resultSet.next()) {
                TableId id = tableId(catalogResolver.apply(resultSet.getString(1)), resultSet.getString(2), resultSet.getString(3));
                tables.add(new TableMetadata(id, resultSet.getString(4)));
            }
        }
        return tables;
    }

    @Override
    public Map<TableId, List<Column>> readColumns(JdbcConnection connection, TableId tableId, ColumnNameFilter columnFilter,
                                                  UnaryOperator<ColumnEditor> columnOverride, ToIntFunction<String> nativeTypeResolver,
                                                  JdbcMetadataReader<Map<TableId, List<Column>>> jdbcReader)
            throws SQLException {
        return jdbcReader.read();
    }

    @Override
    public String tableDdlQuery(TableId tableId) {
        return "SELECT dbms_metadata.get_ddl('TABLE',?,?) FROM DUAL";
    }

    @Override
    public boolean tableDdlUsesParameters() {
        return true;
    }

    @Override
    public void bindTableDdlQuery(PreparedStatement statement, TableId tableId) throws SQLException {
        statement.setString(1, tableId.table());
        statement.setString(2, tableId.schema());
    }

    @Override
    public int tableDdlColumn() {
        return 1;
    }

    @Override
    public String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                                Optional<String> additionalCondition, String orderBy) {
        String sql = select(qualifiedTableName(tableId), projection, condition, additionalCondition, orderBy);
        return " SELECT * FROM (" + sql + ") WHERE ROWNUM <=" + limit;
    }

    static String select(String table, String projection, Optional<String> condition,
                         Optional<String> additionalCondition, String orderBy) {
        StringBuilder sql = new StringBuilder("SELECT ").append(projection).append(" FROM ").append(table);
        condition.ifPresent(value -> sql.append(" WHERE ").append(value));
        additionalCondition.ifPresent(value -> sql.append(condition.isPresent() ? " AND " : " WHERE ").append(value));
        return sql.append(" ORDER BY ").append(orderBy).toString();
    }
}
