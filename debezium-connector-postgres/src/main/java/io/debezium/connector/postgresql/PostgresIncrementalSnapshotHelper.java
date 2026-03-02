/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;

/**
 * Shared utilities for PostgreSQL incremental snapshot implementations.
 */
final class PostgresIncrementalSnapshotHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresIncrementalSnapshotHelper.class);

    private PostgresIncrementalSnapshotHelper() {
    }

    /**
     * Returns an estimated row count for the given table.
     * <p>
     * First tries {@code pg_stat_user_tables.n_live_tup}, a live-tuple estimate maintained by autovacuum.
     * If that value is zero (e.g. autovacuum has not yet run on the table), falls back to
     * {@code pg_class.reltuples}, the planner's row-count estimate which is populated after the first
     * {@code ANALYZE} or table creation. Returns an empty optional only when both sources are unavailable
     * or non-positive.
     *
     * @param connection the PostgreSQL JDBC connection
     * @param tableId    the table to estimate
     * @return an {@link OptionalLong} containing the estimate, or empty if not available
     */
    static OptionalLong estimateRowCount(PostgresConnection connection, TableId tableId) {
        try {
            Long estimate = connection.prepareQueryAndMap(
                    "SELECT CASE WHEN s.n_live_tup > 0 THEN s.n_live_tup" +
                            "          WHEN c.reltuples > 0 THEN c.reltuples::bigint" +
                            "          ELSE 0 END" +
                            " FROM pg_stat_user_tables s" +
                            " JOIN pg_class c ON c.relname = s.relname" +
                            "  AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname)" +
                            " WHERE s.schemaname = ? AND s.relname = ?",
                    statement -> {
                        statement.setString(1, tableId.schema());
                        statement.setString(2, tableId.table());
                    },
                    rs -> rs.next() ? rs.getLong(1) : null);
            if (estimate == null || estimate == 0L) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(estimate);
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to estimate row count for table '{}': {}", tableId, e.getMessage());
            return OptionalLong.empty();
        }
    }
}
