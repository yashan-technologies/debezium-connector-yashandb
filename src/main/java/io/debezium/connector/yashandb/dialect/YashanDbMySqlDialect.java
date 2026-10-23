/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.connector.yashandb.antlr.YashanDbMySqlDdlParser;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

/** MySQL-compatible data SQL over the YasDB JDBC connection. */
public final class YashanDbMySqlDialect implements YashanDbDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbMySqlDialect.class);

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
        return new TableId(database, schema, table);
    }

    @Override
    public String qualifiedTableName(TableId tableId) {
        String database = tableId.schema();
        return database == null || database.isEmpty() ? quote(tableId.table()) : quote(database) + "." + quote(tableId.table());
    }

    public String quote(String identifier) {
        String marker = quoteCharacter();
        return marker + identifier.replace(marker, marker + marker) + marker;
    }

    @Override
    public AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        return new YashanDbMySqlDdlParser(true, includeComments, tableFilter);
    }

    @Override
    public String tableIdsQuery() {
        return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'";
    }

    @Override
    public String tableIdsQuery(String schemaName) {
        return tableIdsQuery() + (schemaName == null ? "" : " AND table_schema = ?");
    }

    @Override
    public String columnsQuery() {
        return "SELECT table_schema, table_name, column_name, column_type, "
                + "character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default, "
                + "ordinal_position, extra FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
    }

    @Override
    public List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName,
                                          JdbcMetadataReader<ResultSet> jdbcReader, UnaryOperator<String> catalogResolver)
            throws SQLException {
        String selectedSchema = schemaName == null ? catalogName : schemaName;
        return connection.prepareQueryAndMap(tableIdsQuery(selectedSchema), statement -> {
            if (selectedSchema != null) {
                statement.setString(1, selectedSchema);
            }
        }, resultSet -> {
            List<TableMetadata> tables = new ArrayList<>();
            while (resultSet.next()) {
                String tableSchema = catalogResolver.apply(resultSet.getString(1));
                if (tableSchema == null || tableSchema.isEmpty()) {
                    tableSchema = selectedSchema;
                }
                tables.add(new TableMetadata(tableId(null, tableSchema, resultSet.getString(2)), "TABLE"));
            }
            return tables;
        });
    }

    @Override
    public Map<TableId, List<Column>> readColumns(JdbcConnection connection, TableId tableId, ColumnNameFilter columnFilter,
                                                  UnaryOperator<ColumnEditor> columnOverride, ToIntFunction<String> nativeTypeResolver,
                                                  JdbcMetadataReader<Map<TableId, List<Column>>> jdbcReader)
            throws SQLException {
        return connection.prepareQueryAndMap(columnsQuery(), statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        }, resultSet -> {
            Map<TableId, List<Column>> columnsByTable = new HashMap<>();
            Set<String> lowercaseColumnNames = new HashSet<>();

            while (resultSet.next()) {
                String columnName = resultSet.getString(3);
                String lowercaseColumnName = columnName.toLowerCase(Locale.ROOT);
                if (!lowercaseColumnNames.add(lowercaseColumnName)) {
                    LOGGER.warn("Table '{}' has columns that differ only by case. Column name: '{}'. "
                            + "Debezium does not support case-sensitive duplicate column names.", tableId, columnName);
                }
                if (columnFilter != null && !columnFilter.matches(null, tableId.schema(), tableId.table(), columnName)) {
                    continue;
                }

                String columnType = resultSet.getString(4);
                if (columnType == null) {
                    columnType = "UNKNOWN";
                }
                String baseType = mySqlBaseType(columnType);

                Integer characterMaximumLength = nullableInteger(resultSet, 5);
                Integer numericPrecision = nullableInteger(resultSet, 6);
                Integer numericScale = nullableInteger(resultSet, 7);
                Integer length = numericPrecision != null ? numericPrecision : characterMaximumLength;
                String defaultValue = resultSet.getString(9);
                String extra = resultSet.getString(11);

                ColumnEditor column = Column.editor()
                        .name(columnName)
                        .type(columnType)
                        .jdbcType(resolveMySqlJdbcType(baseType))
                        .nativeType(nativeTypeResolver.applyAsInt(baseType))
                        .length(length == null ? 0 : length)
                        .optional(isMySqlColumnNullable(resultSet.getString(8)))
                        .position(resultSet.getInt(10))
                        .autoIncremented(hasExtraFlag(extra, "auto_increment"))
                        .generated(hasExtraFlag(extra, "generated"));
                if (numericScale != null) {
                    column.scale(numericScale);
                }

                column = columnOverride.apply(column);
                if (defaultValue != null) {
                    column.defaultValueExpression(defaultValue);
                }
                columnsByTable.computeIfAbsent(tableId, ignored -> new ArrayList<>()).add(column.create());
            }
            return columnsByTable;
        });
    }

    private static Integer nullableInteger(ResultSet resultSet, int columnIndex) throws SQLException {
        Object value = resultSet.getObject(columnIndex);
        if (value == null) {
            return null;
        }
        return value instanceof Number ? ((Number) value).intValue() : Integer.valueOf(value.toString());
    }

    private static boolean isMySqlColumnNullable(String nullable) {
        return nullable == null || "YES".equalsIgnoreCase(nullable) || "1".equals(nullable);
    }

    private static boolean hasExtraFlag(String extra, String flag) {
        return extra != null && extra.toLowerCase(Locale.ROOT).contains(flag);
    }

    private static String mySqlBaseType(String columnType) {
        return columnType.replaceFirst("\\s*\\(.*", "")
                .replaceFirst("(?i)\\s+(unsigned|zerofill).*", "")
                .trim();
    }

    private static int resolveMySqlJdbcType(String typeName) {
        switch (typeName.toLowerCase(Locale.ROOT)) {
            case "bit":
                return Types.BIT;
            case "tinyint":
            case "int1":
                return Types.TINYINT;
            case "smallint":
            case "int2":
                return Types.SMALLINT;
            case "mediumint":
            case "middleint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
                return Types.INTEGER;
            case "bigint":
            case "int8":
                return Types.BIGINT;
            case "decimal":
            case "number":
            case "numeric":
                return Types.NUMERIC;
            case "float":
            case "binary_float":
                return Types.FLOAT;
            case "double":
            case "binary_double":
            case "real":
                return Types.DOUBLE;
            case "bool":
            case "boolean":
                return Types.BOOLEAN;
            case "date":
                return Types.DATE;
            case "time":
                return Types.TIME;
            case "datetime":
            case "timestamp":
                return Types.TIMESTAMP;
            case "timestamp_ltz":
            case "timestamp with local time zone":
                return YasTypes.TIMESTAMP_LTZ;
            case "timestamp_tz":
            case "timestamp with time zone":
                return YasTypes.TIMESTAMP_TZ;
            case "interval year to month":
                return YasTypes.YM_INTERVAL;
            case "interval day to second":
                return YasTypes.DS_INTERVAL;
            case "char":
                return Types.CHAR;
            case "nchar":
                return Types.NCHAR;
            case "varchar":
            case "varchar2":
            case "enum":
            case "set":
                return Types.VARCHAR;
            case "nvarchar":
            case "nvarchar2":
                return Types.NVARCHAR;
            case "binary":
                return Types.BINARY;
            case "varbinary":
                return Types.VARBINARY;
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return Types.BLOB;
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "clob":
                return Types.CLOB;
            case "nclob":
                return Types.NCLOB;
            case "raw":
                return YasTypes.RAW;
            case "xml":
            case "xmltype":
                return YasTypes.SQLXML;
            case "rowid":
                return Types.VARCHAR;
            case "json":
                return YasTypes.JSON;
            default:
                return Types.OTHER;
        }
    }

    private String primaryKeyQuery() {
        return "SELECT column_name FROM information_schema.key_column_usage "
                + "WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position";
    }

    private String uniqueIndexQuery() {
        return "SELECT index_name, column_name, seq_in_index "
                + "FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? "
                + "AND non_unique = 0 AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index";
    }

    @Override
    public List<String> readPrimaryKeyNames(JdbcConnection connection, TableId tableId, JdbcKeyMetadataReader jdbcReader) throws SQLException {
        return connection.prepareQueryAndMap(primaryKeyQuery(), statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        }, resultSet -> {
            List<String> columnNames = new ArrayList<>();
            while (resultSet.next()) {
                columnNames.add(resultSet.getString(1));
            }
            return columnNames;
        });
    }

    @Override
    public List<String> readTableUniqueIndices(JdbcConnection connection, TableId tableId, BiPredicate<String, String> indexFilter,
                                               JdbcKeyMetadataReader jdbcReader)
            throws SQLException {
        return connection.prepareQueryAndMap(uniqueIndexQuery(), statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        }, resultSet -> readUniqueIndexColumns(resultSet, indexFilter));
    }

    private List<String> readUniqueIndexColumns(ResultSet resultSet, BiPredicate<String, String> indexFilter) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        String currentIndexName = null;
        boolean currentIndexIncluded = true;

        while (resultSet.next()) {
            String indexName = resultSet.getString(1);
            String columnName = resultSet.getString(2);
            int columnPosition = resultSet.getInt(3);
            if (indexName == null) {
                continue;
            }

            if (currentIndexName != null && !currentIndexName.equals(indexName)) {
                if (currentIndexIncluded) {
                    return columnNames;
                }
                columnNames.clear();
                currentIndexIncluded = true;
            }
            currentIndexName = indexName;

            if (!indexFilter.test(indexName, columnName)) {
                currentIndexIncluded = false;
                continue;
            }
            if (columnName != null && columnPosition > 0) {
                while (columnNames.size() < columnPosition) {
                    columnNames.add(null);
                }
                columnNames.set(columnPosition - 1, columnName);
            }
        }

        return currentIndexIncluded ? columnNames : Collections.emptyList();
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
