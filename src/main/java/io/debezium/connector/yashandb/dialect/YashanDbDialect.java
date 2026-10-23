/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

/** SQL and identifier rules selected for a single connector task. */
public interface YashanDbDialect {

    String name();

    String quoteCharacter();

    TableId tableId(String database, String schema, String table);

    String qualifiedTableName(TableId tableId);

    /** Creates a parser for the dialect with the connector's comment and table filtering settings. */
    AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter);

    String tableIdsQuery();

    /** Builds a table query with an optional schema filter for dialects using SQL metadata. */
    default String tableIdsQuery(String schemaName) {
        throw new UnsupportedOperationException("SQL table metadata is not supported by dialect " + name());
    }

    /** Returns column metadata SQL with schema and table parameters, when JDBC metadata is not used. */
    default String columnsQuery() {
        throw new UnsupportedOperationException("SQL column metadata is not supported by dialect " + name());
    }

    /** Reads table identities and types using the dialect's metadata source. */
    List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName,
                                   JdbcMetadataReader<ResultSet> jdbcReader, UnaryOperator<String> catalogResolver)
            throws SQLException;

    /** Reads columns while retaining the connection's native type and column overrides. */
    Map<TableId, List<Column>> readColumns(JdbcConnection connection, TableId tableId, ColumnNameFilter columnFilter,
                                           UnaryOperator<ColumnEditor> columnOverride, ToIntFunction<String> nativeTypeResolver,
                                           JdbcMetadataReader<Map<TableId, List<Column>>> jdbcReader)
            throws SQLException;

    record TableMetadata(TableId id, String type) {
    }

    @FunctionalInterface
    interface JdbcMetadataReader<T> {
        T read() throws SQLException;
    }

    /** Reads primary key names, using the JDBC reader unless the dialect supplies its own metadata. */
    default List<String> readPrimaryKeyNames(JdbcConnection connection, TableId tableId, JdbcKeyMetadataReader jdbcReader) throws SQLException {
        return jdbcReader.read();
    }

    /** Reads a unique index, preserving the connection's index and column inclusion rules. */
    default List<String> readTableUniqueIndices(JdbcConnection connection, TableId tableId, BiPredicate<String, String> indexFilter,
                                                JdbcKeyMetadataReader jdbcReader)
            throws SQLException {
        return jdbcReader.read();
    }

    /** Calls the inherited JDBC metadata implementation without re-entering the connection's dialect delegation. */
    @FunctionalInterface
    interface JdbcKeyMetadataReader {
        List<String> read() throws SQLException;
    }

    /** Builds the query used to find the latest DDL SCN for the supplied tables. */
    default String latestTableDdlScnQuery(Collection<TableId> tables) {
        return "SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(last_ddl_time))) FROM all_objects WHERE "
                + tables.stream().map(table -> "(owner = ? AND object_name = ?)").collect(Collectors.joining(" OR "));
    }

    /** Binds the table identifiers used by {@link #latestTableDdlScnQuery(Collection)}. */
    default void bindLatestTableDdlScnQuery(PreparedStatement statement, Collection<TableId> tables) throws SQLException {
        int parameterIndex = 1;
        for (TableId table : tables) {
            statement.setString(parameterIndex++, table.schema());
            statement.setString(parameterIndex++, table.table());
        }
    }

    String tableDdlQuery(TableId tableId);

    default boolean tableDdlUsesParameters() {
        return false;
    }

    /** Binds the table identifier values used by the DDL query, if the query has parameters. */
    default void bindTableDdlQuery(PreparedStatement statement, TableId tableId) throws SQLException {
    }

    int tableDdlColumn();

    String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                         Optional<String> additionalCondition, String orderBy);

    default String snapshotSelect(String table, String columns, String scn) {
        return String.format("SELECT %s FROM %s AS OF SCN %s", columns, table, scn);
    }
}
