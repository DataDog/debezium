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
     * Returns an estimated row count for the given table by querying {@code pg_stat_user_tables.n_live_tup}.
     * This is a catalog-level estimate maintained by autovacuum and does not require a full table scan or lock.
     * Returns an empty optional when the estimate is unavailable or zero (e.g. the table has not yet been
     * analyzed by autovacuum, making a zero value unreliable as a progress estimate).
     *
     * @param connection the PostgreSQL JDBC connection
     * @param tableId    the table to estimate
     * @return an {@link OptionalLong} containing the estimate, or empty if not available
     */
    static OptionalLong estimateRowCount(PostgresConnection connection, TableId tableId) {
        try {
            Long estimate = connection.prepareQueryAndMap(
                    "SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = ? AND relname = ?",
                    statement -> {
                        statement.setString(1, tableId.schema());
                        statement.setString(2, tableId.table());
                    },
                    rs -> rs.next() ? rs.getLong(1) : null);
            // n_live_tup is 0 for tables that have never been analyzed; treat as unknown rather
            // than reporting a misleading zero total.
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
