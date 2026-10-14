/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * JDBC connection for YashanDB.
 */
public class YashanDbConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbConnection.class);

    private static final Field URL = Field.create("url", "Raw JDBC url");

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
        super(config, connectionFactory, initialOperations, quoteCharacter(modeContext), quoteCharacter(modeContext));
        this.modeContext = Objects.requireNonNull(modeContext, "modeContext");
    }

    private static String quoteCharacter(YashanDbModeContext modeContext) {
        return Objects.requireNonNull(modeContext, "modeContext").dialect().quoteCharacter();
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return modeContext.dialect().qualifiedTableName(tableId);
    }

    /**
     * Creates a {@link TableId} with an empty catalog.
     * YashanDB does not use catalog to identify tables — only schema and table name matter.
     * This ensures consistency between schema registration and lookup.
     *
     * @param databaseName the database name (ignored for YashanDB)
     * @param schemaName the schema name
     * @param tableName the table name
     * @return a TableId with empty catalog
     */
    @Override
    public TableId createTableId(String databaseName, String schemaName, String tableName) {
        return modeContext.dialect().tableId("", schemaName, tableName);
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
