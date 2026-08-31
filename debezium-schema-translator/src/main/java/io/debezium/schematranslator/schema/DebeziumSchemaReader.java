package io.debezium.schematranslator.schema;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads Postgres table schemas using Debezium's postgres connector machinery.
 * Mirrors the initialization from {@code PostgresConnectorTask.start()} but strips out
 * everything CDC-related — no replication slots, no streaming, no snapshotter.
 * <p>
 * The connector-level configuration is built once at construction; the JDBC connection
 * is parsed from a {@code postgresql://...} URL and opened fresh on every
 * {@link #readSchemas} call, so each request can target a different database.
 */
public class DebeziumSchemaReader {

    private static final String DEFAULT_SSL_MODE = "prefer";
    private static final int DEFAULT_PG_PORT = 5432;

    private final PostgresConnectorConfig connectorConfig;
    private final TopicNamer topicNamer;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private final Schema sourceInfoSchema;

    @SuppressWarnings("unchecked")
    public DebeziumSchemaReader(String topicPrefix) {
        this.connectorConfig = new PostgresConnectorConfig(staticConfig(topicPrefix));
        this.topicNamer = new TopicNamer(
                connectorConfig.getTopicNamingStrategy(PostgresConnectorConfig.TOPIC_NAMING_STRATEGY));
        this.schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        this.sourceInfoSchema = connectorConfig.getSourceInfoStructMaker().schema();
    }

    /**
     * Reads the Kafka Connect schemas for the specified tables, opening a fresh Postgres
     * connection for the duration of the call.
     *
     * @param connectionString a {@code postgresql://user:password@host:port/dbname[?sslmode=...]} URL
     * @param tableNames       list of table names in "schema.table" or "table" format
     * @return ordered map from TableId to TableSchema
     * @throws RuntimeException if a table is not found or a database error occurs
     */
    public Map<TableId, TableSchema> readSchemas(String connectionString, List<String> tableNames) throws SQLException {
        JdbcConfiguration jdbcConfig = parseJdbcConfig(connectionString);

        List<TableId> requestedIds = tableNames.stream()
                .map(TopicNamer::parseTableId)
                .toList();
        Set<TableId> requestedSet = Set.copyOf(requestedIds);
        Tables.TableFilter filter = Tables.TableFilter.fromPredicate(requestedSet::contains);

        Charset databaseCharset;
        try (PostgresConnection temp = new PostgresConnection(jdbcConfig, PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = temp.getDatabaseCharset();
        }

        PostgresValueConverterBuilder vcBuilder = (typeRegistry) -> PostgresValueConverter.of(
                connectorConfig, databaseCharset, typeRegistry);

        try (PostgresConnection connection = new PostgresConnection(jdbcConfig, vcBuilder, PostgresConnection.CONNECTION_GENERAL)) {
            PostgresDefaultValueConverter defaultValueConverter = connection.getDefaultValueConverter();
            PostgresValueConverter valueConverter = vcBuilder.build(connection.getTypeRegistry());
            TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                    valueConverter, defaultValueConverter, schemaNameAdjuster,
                    new CustomConverterRegistry(null), sourceInfoSchema,
                    connectorConfig.getFieldNamer(), false);

            Tables tables = new Tables();
            connection.readSchema(tables, null, null, filter, null, true);

            Map<TableId, TableSchema> result = new LinkedHashMap<>();
            for (TableId tableId : requestedIds) {
                Table table = tables.forTable(tableId);
                if (table == null) {
                    throw new RuntimeException("Table not found: " + tableId);
                }
                TableSchema tableSchema = tableSchemaBuilder.create(
                        topicNamer.strategy(), table, null, null, null);
                result.put(tableId, tableSchema);
            }
            return result;
        }
    }

    /**
     * Parses a Postgres connection URL ({@code postgresql://user:pass@host:port/dbname[?sslmode=...]})
     * into a {@link JdbcConfiguration} suitable for opening a {@link PostgresConnection}.
     */
    static JdbcConfiguration parseJdbcConfig(String connectionString) {
        if (connectionString == null || connectionString.isBlank()) {
            throw new IllegalArgumentException("connection_string must not be empty");
        }
        URI uri;
        try {
            uri = new URI(connectionString);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid connection_string: " + e.getMessage(), e);
        }
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equals("postgresql") || scheme.equals("postgres"))) {
            throw new IllegalArgumentException(
                    "connection_string must start with 'postgresql://' or 'postgres://'");
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("connection_string is missing the host");
        }
        int port = uri.getPort() == -1 ? DEFAULT_PG_PORT : uri.getPort();
        String path = uri.getPath();
        if (path == null || path.length() <= 1) {
            throw new IllegalArgumentException("connection_string is missing the database name");
        }
        String database = path.substring(1);

        String user = "postgres";
        String password = "";
        String userInfo = uri.getUserInfo();
        if (userInfo != null && !userInfo.isEmpty()) {
            int colon = userInfo.indexOf(':');
            if (colon >= 0) {
                user = URLDecoder.decode(userInfo.substring(0, colon), StandardCharsets.UTF_8);
                password = URLDecoder.decode(userInfo.substring(colon + 1), StandardCharsets.UTF_8);
            }
            else {
                user = URLDecoder.decode(userInfo, StandardCharsets.UTF_8);
            }
        }

        String sslMode = DEFAULT_SSL_MODE;
        String query = uri.getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                int eq = param.indexOf('=');
                if (eq > 0) {
                    String key = param.substring(0, eq);
                    String value = URLDecoder.decode(param.substring(eq + 1), StandardCharsets.UTF_8);
                    if ("sslmode".equals(key)) {
                        sslMode = value;
                    }
                }
            }
        }

        return JdbcConfiguration.create()
                .withHostname(host)
                .withPort(port)
                .withDatabase(database)
                .withUser(user)
                .withPassword(password)
                .with("sslmode", sslMode)
                .build();
    }

    /**
     * Builds the static, connector-level configuration. Connection details are not part of
     * this config — they come per-request via {@link #parseJdbcConfig(String)}.
     */
    private static Configuration staticConfig(String topicPrefix) {
        return Configuration.create()
                .with("topic.prefix", topicPrefix)
                // Required for Avro-compatible schema names
                .with("schema.name.adjustment.mode", "avro")
                // Required by config validation, never used at runtime
                .with("plugin.name", "pgoutput")
                .with("slot.name", "dummy_slot")
                .build();
    }

    public TopicNamer getTopicNamer() {
        return topicNamer;
    }
}
