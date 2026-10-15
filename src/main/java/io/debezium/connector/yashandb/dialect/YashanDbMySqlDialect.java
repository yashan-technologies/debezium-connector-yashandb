/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.relational.TableId;

/** MySQL-compatible data SQL over the YasDB JDBC connection. */
public final class YashanDbMySqlDialect implements YashanDbDialect {

    @Override
    public String name() {
        return "mysql";
    }

    @Override
    public String quoteCharacter() {
        return String.valueOf((char) 96);
    }

    @Override
    public TableId tableId(String database, String schema, String table) {
        return new TableId("", schema, table);
    }

    @Override
    public String qualifiedTableName(TableId tableId) {
        String database = tableId.catalog() != null && !tableId.catalog().isEmpty() ? tableId.catalog() : tableId.schema();
        return database == null || database.isEmpty() ? quote(tableId.table()) : quote(database) + "." + quote(tableId.table());
    }

    public String quote(String identifier) {
        String marker = quoteCharacter();
        return marker + identifier.replace(marker, marker + marker) + marker;
    }

    @Override
    public String tableIdsQuery() {
        return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'";
    }

    @Override
    public String latestTableDdlScnQuery(Collection<TableId> tables) {
        return "SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(update_time))) FROM INFORMATION_SCHEMA.tables WHERE "
                + tables.stream().map(table -> "(table_schema = ? AND table_name = ?)")
                        .collect(Collectors.joining(" OR "));
    }

    @Override
    public void bindLatestTableDdlScnQuery(PreparedStatement statement, Collection<TableId> tables) throws SQLException {
        int parameterIndex = 1;
        for (TableId table : tables) {
            statement.setString(parameterIndex++, table.schema());
            statement.setString(parameterIndex++, table.table());
        }
    }

    @Override
    public String tableDdlQuery(TableId tableId) {
        return "SHOW CREATE TABLE " + qualifiedTableName(tableId);
    }

    @Override
    public int tableDdlColumn() {
        return 2;
    }

    @Override
    public String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                                Optional<String> additionalCondition, String orderBy) {
        return YashanDbNativeDialect.select(qualifiedTableName(tableId), projection, condition, additionalCondition, orderBy)
                + " LIMIT " + limit;
    }
}
