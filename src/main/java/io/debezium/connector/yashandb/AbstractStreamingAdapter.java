/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Abstract implementation of the {@link StreamingAdapter} for which all streaming adapters are derived.
 *
 * @author Chris Cranford
 */
public abstract class AbstractStreamingAdapter implements StreamingAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamingAdapter.class);

    /**
     * YashanDB bind parameter limit
     */
    private static final int MAX_TABLES_PER_BATCH = 500;

    protected final YashanDBConnectorConfig connectorConfig;

    public AbstractStreamingAdapter(YashanDBConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    protected Scn resolveScn(Document document) {
        final String scn = document.getString(SourceInfo.SCN_KEY);
        if (scn == null) {
            Long scnValue = document.getLong(SourceInfo.SCN_KEY);
            return Scn.valueOf(scnValue == null ? 0 : scnValue);
        }
        return Scn.valueOf(scn);
    }

    /**
     * Checks whether the two specified system change numbers have the same timestamp.
     *
     * @param scn1 first scn number, may be {@code null}
     * @param scn2 second scn number, may be {@code null}
     * @param connection the database connection, must not be {@code null}
     * @return true if the two system change numbers have the same timestamp; false otherwise
     * @throws SQLException if a database error occurred
     */
    protected boolean areSameTimestamp(Scn scn1, Scn scn2, YashanDBConnection connection) throws SQLException {
        if (scn1 == null) {
            return false;
        }
        if (scn2 == null) {
            return false;
        }

        final String query = "SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(?)=SCN_TO_TIMESTAMP(?)";
        try (PreparedStatement ps = connection.connection().prepareStatement(query)) {
            ps.setLong(1, scn1.longValue());
            ps.setLong(2, scn2.longValue());
            ResultSet rs = ps.executeQuery();
            return rs.next();
        }
    }

    /**
     * Returns the SCN of the latest DDL change to the captured tables.
     * The result will be empty if there is no table to capture as per the configuration.
     * 查询最近的DDL变更的SCN
     * @param ctx the snapshot contest, must not be {@code null}
     * @param connection the database connection, must not be {@code null}
     * @return the latest table DDL system change number, never {@code null} but may be empty.
     * @throws SQLException if a database error occurred
     */
    protected Optional<Scn> getLatestTableDdlScn(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx, YashanDBConnection connection)
            throws SQLException {
        if (ctx.capturedTables.isEmpty()) {
            return Optional.empty();
        }

        // Split into batches to avoid exceeding YashanDB's bind parameter limit (32000)
        List<List<TableId>> batches = partitionList(new ArrayList<>(ctx.capturedTables), MAX_TABLES_PER_BATCH);

        Scn maxScn = null;
        for (List<TableId> batch : batches) {
            Optional<Scn> batchResult = queryBatchDdlScn(batch, connection);
            if (batchResult.isPresent()) {
                if (maxScn == null || batchResult.get().compareTo(maxScn) > 0) {
                    maxScn = batchResult.get();
                }
            }
        }

        return Optional.ofNullable(maxScn);
    }

    private Optional<Scn> queryBatchDdlScn(List<TableId> tables, YashanDBConnection connection) throws SQLException {
        final String lastDdlScnQuery = "SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(last_ddl_time)))" +
                " FROM all_objects" +
                " WHERE" +
                tables.stream()
                        .map(t -> " (owner=? and object_name=?)")
                        .collect(Collectors.joining(" OR"));

        try (PreparedStatement stmt = connection.connection().prepareStatement(lastDdlScnQuery)) {
            int paramIndex = 1;
            for (TableId table : tables) {
                stmt.setString(paramIndex++, table.schema());
                stmt.setString(paramIndex++, table.table());
            }
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get latest table DDL SCN");
            }

            // Guard against LAST_DDL_TIME with value of 0.
            // This case should be treated as if we were unable to determine a value for LAST_DDL_TIME.
            // This forces later calculations to be based upon the current SCN.
            String latestDdlTime = rs.getString(1);
            if ("0".equals(latestDdlTime)) {
                return Optional.empty();
            }

            return Optional.of(Scn.valueOf(latestDdlTime));
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 8180) {
                // DBZ-1446 In this use case we actually do not want to propagate the exception but
                // rather return an empty optional value allowing the current SCN to take prior.
                LOGGER.info("No latest table SCN could be resolved, defaulting to current SCN");
                return Optional.empty();
            }
            throw e;
        }
    }

    private <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return partitions;
    }
}
