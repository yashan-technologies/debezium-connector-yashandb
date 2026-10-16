/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasRowID;
import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.spi.schema.DataCollectionId;

/**
 * JDBC connection for YashanDB.
 */
public class YashanDbConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbConnection.class);

    private static final Field URL = Field.create("url", "Raw JDBC url");
    private static final String MYSQL_TABLES_QUERY = "SELECT table_schema, table_name FROM information_schema.tables "
            + "WHERE table_type = 'BASE TABLE'";
    private static final String MYSQL_TABLES_BY_SCHEMA_QUERY = MYSQL_TABLES_QUERY + " AND table_schema = ?";
    private static final String MYSQL_COLUMNS_QUERY = "SELECT table_schema, table_name, column_name, data_type, "
            + "character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default, "
            + "ordinal_position, extra FROM information_schema.columns "
            + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
    private static final String MYSQL_PRIMARY_KEY_QUERY = "SELECT column_name FROM information_schema.key_column_usage "
            + "WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position";
    private static final String MYSQL_UNIQUE_INDEX_QUERY = "SELECT index_name, column_name, seq_in_index "
            + "FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? "
            + "AND non_unique = 0 AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index";

    private static final int YASHANDB_UNSET_SCALE = -127;
    private final YashanDbModeContext modeContext;

    /**
     * Creates a new YashanDB connection.
     *
     * @param config the JDBC configuration
     */
    public YashanDbConnection(JdbcConfiguration config) {
        this(config, new YashanDbModeContext(false));
    }

    public YashanDbConnection(JdbcConfiguration config, YashanDbModeContext modeContext) {
        this(config, resolveConnectionFactory(config), null, modeContext);
    }

    /**
     * Creates a new YashanDB connection with a custom connection factory.
     *
     * @param config the JDBC configuration
     * @param connectionFactory the connection factory
     */
    public YashanDbConnection(JdbcConfiguration config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, null, new YashanDbModeContext(false));
    }

    /**
     * Creates a new YashanDB connection with initial operations.
     *
     * @param config the JDBC configuration
     * @param connectionFactory the connection factory
     * @param initialOperations the initial operations to execute
     */
    protected YashanDbConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, Operations initialOperations) {
        this(config, connectionFactory, initialOperations, new YashanDbModeContext(false));
    }

    private YashanDbConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, Operations initialOperations,
                               YashanDbModeContext modeContext) {
        super(connectionConfiguration(config, modeContext), connectionFactory, initialOperations, quoteCharacter(modeContext), quoteCharacter(modeContext));
        this.modeContext = Objects.requireNonNull(modeContext, "modeContext");
    }

    private static JdbcConfiguration connectionConfiguration(JdbcConfiguration config, YashanDbModeContext modeContext) {
        if (Objects.requireNonNull(modeContext, "modeContext").isMySql()) {
            return JdbcConfiguration.copy(config).with("compatVector", "mysql").build();
        }
        return config;
    }

    private static String quoteCharacter(YashanDbModeContext modeContext) {
        return Objects.requireNonNull(modeContext, "modeContext").dialect().quoteCharacter();
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return modeContext.dialect().qualifiedTableName(tableId);
    }

    /**
     * Creates a {@link TableId} using the selected database mode's identifier fields.
     * This ensures consistency between schema registration and lookup.
     *
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param tableName the table name
     * @return a table identifier in the selected database mode
     */
    @Override
    public TableId createTableId(String databaseName, String schemaName, String tableName) {
        return modeContext.dialect().tableId(databaseName, schemaName, tableName);
    }

    @Override
    public void readSchema(Tables tables, String catalogName, String schemaName,
                           TableFilter tableFilter, ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());
        DatabaseMetaData metadata = connection().getMetaData();
        Set<TableId> tableIds = new HashSet<>();
        Map<TableId, List<Attribute>> attributesByTable = new HashMap<>();
        String schemaNamePattern = modeContext.isMySql() ? null : createPatternFromName(schemaName, metadata.getSearchStringEscape());
        String metadataCatalog = modeContext.isMySql() && catalogName == null ? schemaName : catalogName;
        String metadataSchemaPattern = modeContext.isMySql() ? null : schemaNamePattern;

        if (modeContext.isMySql()) {
            tableIds.addAll(getMySqlTableIds(metadataCatalog, tableFilter, attributesByTable));
        }
        else {
            try (ResultSet rs = metadata.getTables(metadataCatalog, metadataSchemaPattern, null, supportedTableTypes())) {
                while (rs.next()) {
                    String metaCatalogName = resolveCatalogName(rs.getString(1));
                    String metaSchemaName = rs.getString(2);
                    String metaTableName = rs.getString(3);
                    String tableType = rs.getString(4);
                    if (isTableType(tableType)) {
                        TableId tableId = createTableId(metaCatalogName, metaSchemaName, metaTableName);
                        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                            tableIds.add(tableId);
                            attributesByTable.putAll(getAttributeDetails(tableId, tableType));
                        }
                    }
                }
            }
        }
        LOGGER.debug("{} table(s) will be scanned", tableIds.size());

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        for (TableId tableId : tableIds) {
            LOGGER.debug("Retrieving columns of table {}", tableId);
            Map<TableId, List<Column>> columns;
            if (modeContext.isMySql()) {
                columns = getMySqlColumnsDetails(tableId, columnFilter);
            }
            else {
                columns = getColumnsDetails(catalogName, tableId.schema(), tableId.table(),
                        tableFilter, columnFilter, metadata, Collections.emptySet());
            }
            columnsByTable.putAll(columns);
        }

        if (columnsByTable.isEmpty()) {
            for (TableId emptyTableId : tableIds) {
                LOGGER.warn("Table {} has no column or all columns were excluded due to include/exclude lists.", emptyTableId);
                columnsByTable.put(emptyTableId, new ArrayList<>());
            }
        }

        for (Map.Entry<TableId, List<Column>> entry : columnsByTable.entrySet()) {
            TableId tableId = entry.getKey();
            List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, tableId);
            List<Column> columns = entry.getValue();
            Collections.sort(columns);
            List<Attribute> attributes = attributesByTable.getOrDefault(tableId, Collections.emptyList());
            tables.overwriteTable(tableId, columns, pkColumnNames, null, attributes);
        }

        if (removeTablesNotFoundInJdbc) {
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    private Set<TableId> getMySqlTableIds(String catalogName, TableFilter tableFilter,
                                          Map<TableId, List<Attribute>> attributesByTable)
            throws SQLException {
        String query = catalogName == null ? MYSQL_TABLES_QUERY : MYSQL_TABLES_BY_SCHEMA_QUERY;
        return prepareQueryAndMap(query, statement -> {
            if (catalogName != null) {
                statement.setString(1, catalogName);
            }
        }, resultSet -> {
            Set<TableId> tableIds = new HashSet<>();
            while (resultSet.next()) {
                String tableCatalog = resolveCatalogName(resultSet.getString(1));
                if (tableCatalog == null || tableCatalog.isEmpty()) {
                    tableCatalog = catalogName;
                }
                String tableName = resultSet.getString(2);
                TableId tableId = createTableId(tableCatalog, null, tableName);
                if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                    tableIds.add(tableId);
                    attributesByTable.putAll(getAttributeDetails(tableId, "TABLE"));
                }
            }
            return tableIds;
        });
    }

    private Map<TableId, List<Column>> getMySqlColumnsDetails(TableId tableId, ColumnNameFilter columnFilter)
            throws SQLException {
        return prepareQueryAndMap(MYSQL_COLUMNS_QUERY, statement -> {
            statement.setString(1, tableId.catalog());
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
                if (columnFilter != null && !columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
                    continue;
                }

                String typeName = resultSet.getString(4);
                if (typeName == null) {
                    typeName = "UNKNOWN";
                }

                Integer characterMaximumLength = nullableInteger(resultSet, 5);
                Integer numericPrecision = nullableInteger(resultSet, 6);
                Integer numericScale = nullableInteger(resultSet, 7);
                Integer length = numericPrecision != null ? numericPrecision : characterMaximumLength;
                String defaultValue = resultSet.getString(9);
                String extra = resultSet.getString(11);

                ColumnEditor column = Column.editor()
                        .name(columnName)
                        .type(typeName)
                        .jdbcType(resolveMySqlJdbcType(typeName))
                        .nativeType(resolveNativeType(typeName))
                        .length(length == null ? 0 : length)
                        .optional(isMySqlColumnNullable(resultSet.getString(8)))
                        .position(resultSet.getInt(10))
                        .autoIncremented(hasExtraFlag(extra, "auto_increment"))
                        .generated(hasExtraFlag(extra, "generated"));
                if (numericScale != null) {
                    column.scale(numericScale);
                }

                column = overrideColumn(column);
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

    private static int resolveMySqlJdbcType(String typeName) {
        switch (typeName.toLowerCase(Locale.ROOT)) {
            case "bit":
                return Types.BIT;
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "mediumint":
            case "int":
            case "integer":
                return Types.INTEGER;
            case "bigint":
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

    @Override
    public List<String> readPrimaryKeyNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        if (modeContext.isMySql()) {
            TableId mysqlTableId = metadataTableId(id);
            return prepareQueryAndMap(MYSQL_PRIMARY_KEY_QUERY, statement -> {
                statement.setString(1, mysqlTableId.catalog());
                statement.setString(2, mysqlTableId.table());
            }, resultSet -> {
                List<String> columnNames = new ArrayList<>();
                while (resultSet.next()) {
                    columnNames.add(resultSet.getString(1));
                }
                return columnNames;
            });
        }
        return super.readPrimaryKeyNames(metadata, metadataTableId(id));
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        if (modeContext.isMySql()) {
            TableId mysqlTableId = metadataTableId(id);
            return prepareQueryAndMap(MYSQL_UNIQUE_INDEX_QUERY, statement -> {
                statement.setString(1, mysqlTableId.catalog());
                statement.setString(2, mysqlTableId.table());
            }, resultSet -> readMySqlUniqueIndexColumns(resultSet));
        }
        return super.readTableUniqueIndices(metadata, metadataTableId(id));
    }

    private List<String> readMySqlUniqueIndexColumns(ResultSet resultSet) throws SQLException {
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

            if (!isTableUniqueIndexIncluded(indexName, columnName)) {
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

    private TableId metadataTableId(TableId id) {
        return modeContext.isMySql() ? new TableId(id.catalog(), null, id.table()) : id;
    }

    public boolean isMySqlMode() {
        return modeContext.isMySql();
    }

    /**
     * Retrieves the current system change number from the database.
     *
     * @return the current SCN
     * @throws SQLException if the query fails
     */
    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

    /**
     * Queries the oldest transaction start SCN from open transactions.
     *
     * @return the oldest transaction start SCN, or NULL if no open transactions
     * @throws SQLException if the query fails
     */
    public Scn queryOldTransactionStartScn() throws SQLException {
        return queryAndMap("SELECT MIN(START_SCN) FROM SYS.V_$TRANSACTION WHERE STATUS = 'OPEN'", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getLong(1));
            }
            else {
                return Scn.NULL;
            }
        });
    }

    private static ConnectionFactory resolveConnectionFactory(JdbcConfiguration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config));
    }

    /**
     * Builds the JDBC connection string from the configuration.
     *
     * @param config the JDBC configuration
     * @return the connection string
     */
    public static String connectionString(JdbcConfiguration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : String.format("jdbc:yasdb://%s:%s/%s", config.getString(JdbcConfiguration.HOSTNAME), config.getString(JdbcConfiguration.PORT),
                        config.getString(JdbcConfiguration.DATABASE));
    }

    /**
     * Retrieves the DDL metadata for the specified table.
     *
     * @param tableId the table identifier
     * @return the DDL statement for the table
     * @throws SQLException if the query fails
     */
    public String getTableMetadataDdl(TableId tableId) throws SQLException {
        String sql = modeContext.dialect().tableDdlQuery(tableId);
        if (modeContext.dialect().tableDdlUsesParameters()) {
            return prepareQueryAndMap(sql, ps -> modeContext.dialect().bindTableDdlQuery(ps, tableId),
                    rs -> readTableDdl(rs, tableId));
        }
        return queryAndMap(sql, rs -> readTableDdl(rs, tableId));
    }

    private String readTableDdl(ResultSet result, TableId tableId) throws SQLException {
        if (!result.next()) {
            throw new DebeziumException("Could not get DDL metadata for table: " + tableId);
        }
        return result.getString(modeContext.dialect().tableDdlColumn());
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // "NULLS LAST is the default for ascending order"
        // https://doc.yashandb.com/yashandb/23.4/zh/All-Manuals/Development-Guide/SQL-Reference-Manual/SQL-Statements/SELECT.html
        return Optional.of(true);
    }

    @Override
    public String buildSelectWithRowLimits(TableId tableId,
                                           int limit,
                                           String projection,
                                           Optional<String> condition,
                                           Optional<String> additionalCondition,
                                           String orderBy) {
        return modeContext.dialect().limitedSelect(tableId, limit, projection, condition, additionalCondition, orderBy);
    }

    protected boolean isArchiveLogMode() {
        try {
            final String mode = queryAndMap("SELECT LOG_MODE FROM V$DATABASE", rs -> rs.next() ? rs.getString(1) : "");
            LOGGER.debug("LOG_MODE={}", mode);
            return "ARCHIVELOG".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to YashanDB and looking at LOG_MODE mode: ", e);
        }
    }

    /**
     * Retrieves all table IDs from the specified catalog.
     *
     * @param catalogName the catalog name
     * @return the set of table IDs
     * @throws SQLException if the query fails
     */
    public Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = modeContext.dialect().tableIdsQuery();

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(modeContext.dialect().tableId(null, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    /**
     * Converts an SCN to a timestamp.
     *
     * @param scn the system change number
     * @return the corresponding timestamp, or empty if conversion fails
     * @throws SQLException if the query fails
     */
    public Optional<Instant> getScnToTimestamp(Scn scn) throws SQLException {
        try {
            return queryAndMap("SELECT scn_to_timestamp(" + scn + ") FROM DUAL", rs -> rs.next()
                    ? Optional.of(rs.getTimestamp(1).toInstant())
                    : Optional.empty());
        }
        catch (SQLException e) {
            // Any other SQLException should be thrown
            throw e;
        }
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        // YashanDB JDBC driver returns YasRowID (not Serializable) for ROWID columns via rs.getObject().
        // Incremental snapshot serializes key arrays to offset storage using ObjectOutputStream,
        // which fails with NotSerializableException on YasRowID.
        // Convert to String here so the value flows correctly through:
        // - offset serialization (String is Serializable)
        // - SQL binding via PreparedStatement.setObject() (ROWID string comparison)
        // - change event emission (YashanDbValueConverters already maps ROWID to STRING schema)
        final Object value = super.getColumnValue(rs, columnIndex, column, table);
        if (value instanceof YasRowID) {
            return value.toString();
        }
        return value;
    }

    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        // This allows the column state to be overridden before default-value resolution so that the
        // output of the default value is within the same precision as that of the column values.
        if (YasTypes.NUMBER == column.jdbcType() || YasTypes.DECIMAL == column.jdbcType()) {
            column.scale().filter(s -> s == YASHANDB_UNSET_SCALE).ifPresent(s -> column.scale(null));
        }
        return column;
    }

    /**
     * Retrieves the SQL keywords from the JDBC driver metadata.
     *
     * @return the list of SQL keywords
     */
    public List<String> getSQLKeywords() {
        try {
            return Arrays.asList(connection().getMetaData().getSQLKeywords().split(","));
        }
        catch (SQLException e) {
            LOGGER.debug("Failed to acquire SQL keywords from JDBC driver.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public <T extends DataCollectionId> ChunkQueryBuilder<T> chunkQueryBuilder(RelationalDatabaseConnectorConfig connectorConfig) {
        return new YashanDbPhysicalRowIdentifierChunkQueryBuilder<>(connectorConfig, this);
    }
}
