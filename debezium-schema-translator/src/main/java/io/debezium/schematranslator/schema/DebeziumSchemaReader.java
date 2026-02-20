/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.schema;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Reads PostgreSQL table schemas using Debezium's postgres connector machinery.
 * Mirrors the initialization from {@code PostgresConnectorTask.start()} but strips out
 * everything CDC-related — no replication slots, no streaming, no snapshotter.
 *
 * The PostgreSQL connection is initialized lazily on the first call to {@link #readSchemas},
 * so the service can start without PostgreSQL being available. The connection is then
 * reused for subsequent calls.
 */
public class DebeziumSchemaReader implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumSchemaReader.class);

    private final PostgresConnectorConfig connectorConfig;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;

    // Lazily initialized on first readSchemas() call, then reused
    private volatile PostgresConnection pgConnection;
    private volatile TableSchemaBuilder tableSchemaBuilder;

    @SuppressWarnings("unchecked")
    public DebeziumSchemaReader(Configuration config) {
        this.connectorConfig = new PostgresConnectorConfig(config);
        this.topicNamingStrategy = connectorConfig.getTopicNamingStrategy(
                PostgresConnectorConfig.TOPIC_NAMING_STRATEGY);
    }

    /**
     * Reads the Kafka Connect schemas for the specified tables.
     *
     * @param tableNames list of table names in "schema.table" or "table" format
     * @return ordered map from TableId to TableSchema
     * @throws RuntimeException if a table is not found or a database error occurs
     */
    public Map<TableId, TableSchema> readSchemas(List<String> tableNames) throws SQLException {
        ensureConnected();

        // Parse table names, defaulting schema to "public" when not specified
        // (mirrors PostgresSchema.parse() which is protected)
        List<TableId> requestedIds = tableNames.stream()
                .map(DebeziumSchemaReader::parseTableId)
                .collect(Collectors.toList());

        Set<TableId> requestedSet = Set.copyOf(requestedIds);
        Tables.TableFilter filter = Tables.TableFilter.fromPredicate(requestedSet::contains);

        // Read schema metadata from JDBC
        Tables tables = new Tables();
        pgConnection.readSchema(tables, null, null, filter, null, true);

        Map<TableId, TableSchema> result = new LinkedHashMap<>();
        for (TableId tableId : requestedIds) {
            Table table = tables.forTable(tableId);
            if (table == null) {
                throw new RuntimeException("Table not found: " + tableId);
            }
            TableSchema tableSchema = tableSchemaBuilder.create(
                    topicNamingStrategy, table, null, null, null);
            result.put(tableId, tableSchema);
        }
        return result;
    }

    /**
     * Initializes the PostgreSQL connection and related components on the first call.
     * Subsequent calls are no-ops if the connection is already established.
     * Uses double-checked locking to avoid redundant initialization under concurrency.
     */
    private void ensureConnected() {
        if (pgConnection != null) {
            return;
        }
        synchronized (this) {
            if (pgConnection != null) {
                return;
            }
            LOGGER.info("Initializing PostgreSQL connection");
            initConnection();
        }
    }

    private void initConnection() {
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // Step 1: Get database charset (same pattern as PostgresConnectorTask line 109)
        final Charset databaseCharset;
        try (PostgresConnection temp = new PostgresConnection(
                connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = temp.getDatabaseCharset();
        }

        // Step 2: Build value converter builder
        final PostgresValueConverterBuilder vcBuilder = (typeRegistry) -> PostgresValueConverter.of(
                connectorConfig, databaseCharset, typeRegistry);

        // Step 3: Create main connection (initialises TypeRegistry internally)
        final PostgresConnection connection = new PostgresConnection(
                connectorConfig.getJdbcConfig(), vcBuilder, PostgresConnection.CONNECTION_GENERAL);

        // Step 4: Extract components
        final PostgresDefaultValueConverter defaultValueConverter = connection.getDefaultValueConverter();
        final PostgresValueConverter valueConverter = vcBuilder.build(connection.getTypeRegistry());

        // Step 5: Build TableSchemaBuilder (mirrors PostgresSchema.getTableSchemaBuilder())
        final Schema sourceInfoSchema = connectorConfig.getSourceInfoStructMaker().schema();
        final CustomConverterRegistry customConverterRegistry = new CustomConverterRegistry(null);
        this.tableSchemaBuilder = new TableSchemaBuilder(
                valueConverter, defaultValueConverter, schemaNameAdjuster,
                customConverterRegistry, sourceInfoSchema,
                connectorConfig.getFieldNamer(), false);

        // Assign last so that pgConnection != null only once fully initialized
        this.pgConnection = connection;
        LOGGER.info("PostgreSQL connection initialized successfully");
    }

    /**
     * Parses a table name string into a {@link TableId}, defaulting the schema to {@code "public"}
     * when no schema is specified. Mirrors the logic of {@code PostgresSchema.parse()}.
     */
    private static TableId parseTableId(String table) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        return tableId.schema() == null
                ? new TableId(tableId.catalog(), "public", tableId.table())
                : tableId;
    }

    /**
     * Returns the topic naming strategy, used to derive subjects for SR registration.
     */
    public TopicNamingStrategy<TableId> getTopicNamingStrategy() {
        return topicNamingStrategy;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (pgConnection != null) {
                try {
                    pgConnection.close();
                }
                catch (Exception e) {
                    LOGGER.warn("Error closing PostgresConnection", e);
                }
                pgConnection = null;
                tableSchemaBuilder = null;
            }
        }
    }
}
