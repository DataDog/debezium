package io.debezium.schematranslator.schema;

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
import java.util.Properties;
import java.util.Set;

/**
 * Reads Postgres table schemas using Debezium's postgres connector machinery.
 * Mirrors the initialization from {@code PostgresConnectorTask.start()} but strips out
 * everything CDC-related — no replication slots, no streaming, no snapshotter.
 * <p>
 * The Postgres connection is opened on every {@link #readSchemas} call and closed
 * before returning, so each request can target a different database.
 */
public class DebeziumSchemaReader {

    private static final String DEFAULT_SSL_MODE = "prefer";
    private static final int DEFAULT_PG_PORT = 5432;

    private final String topicPrefix;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;

    @SuppressWarnings("unchecked")
    public DebeziumSchemaReader(String topicPrefix) {
        this.topicPrefix = topicPrefix;
        // The topic naming strategy depends only on the topic prefix, so build it once here
        // from a config that has no Postgres connection details.
        PostgresConnectorConfig baseConfig = new PostgresConnectorConfig(Configuration.from(baseProps(topicPrefix)));
        this.topicNamingStrategy = baseConfig.getTopicNamingStrategy(PostgresConnectorConfig.TOPIC_NAMING_STRATEGY);
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
        // PostgresConnectorConfig construction reflectively loads classes (e.g. PostgresSourceInfoStructMaker)
        // via the thread's context classloader. HTTP server worker threads inherit a classloader that doesn't
        // see those classes, so we pin the loader to this class's for the duration of the call.
        Thread current = Thread.currentThread();
        ClassLoader previousLoader = current.getContextClassLoader();
        current.setContextClassLoader(DebeziumSchemaReader.class.getClassLoader());
        try {
            return doReadSchemas(connectionString, tableNames);
        }
        finally {
            current.setContextClassLoader(previousLoader);
        }
    }

    private Map<TableId, TableSchema> doReadSchemas(String connectionString, List<String> tableNames) throws SQLException {
        Configuration config = buildConfig(connectionString, topicPrefix);
        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);

        List<TableId> requestedIds = tableNames.stream()
                .map(DebeziumSchemaReader::parseTableId)
                .toList();
        Set<TableId> requestedSet = Set.copyOf(requestedIds);
        Tables.TableFilter filter = Tables.TableFilter.fromPredicate(requestedSet::contains);

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final Charset databaseCharset;
        try (PostgresConnection temp = new PostgresConnection(
                connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = temp.getDatabaseCharset();
        }

        final PostgresValueConverterBuilder vcBuilder = (typeRegistry) -> PostgresValueConverter.of(
                connectorConfig, databaseCharset, typeRegistry);

        try (PostgresConnection connection = new PostgresConnection(
                connectorConfig.getJdbcConfig(), vcBuilder, PostgresConnection.CONNECTION_GENERAL)) {

            final PostgresDefaultValueConverter defaultValueConverter = connection.getDefaultValueConverter();
            final PostgresValueConverter valueConverter = vcBuilder.build(connection.getTypeRegistry());
            final Schema sourceInfoSchema = connectorConfig.getSourceInfoStructMaker().schema();
            final CustomConverterRegistry customConverterRegistry = new CustomConverterRegistry(null);
            TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                    valueConverter, defaultValueConverter, schemaNameAdjuster,
                    customConverterRegistry, sourceInfoSchema,
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
                        topicNamingStrategy, table, null, null, null);
                result.put(tableId, tableSchema);
            }
            return result;
        }
    }

    /**
     * Parses a Postgres connection URL ({@code postgresql://user:pass@host:port/dbname[?sslmode=...]})
     * and produces a Debezium {@link Configuration} suitable for instantiating a
     * {@link PostgresConnectorConfig}.
     */
    static Configuration buildConfig(String connectionString, String topicPrefix) {
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

        Properties props = baseProps(topicPrefix);
        props.put("database.hostname", host);
        props.put("database.port", String.valueOf(port));
        props.put("database.dbname", database);
        props.put("database.user", user);
        props.put("database.password", password);
        props.put("database.sslmode", sslMode);
        return Configuration.from(props);
    }

    private static Properties baseProps(String topicPrefix) {
        Properties props = new Properties();
        props.put("topic.prefix", topicPrefix);
        // Required for Avro-compatible schema names
        props.put("schema.name.adjustment.mode", "avro");
        // Required by config validation, never used at runtime
        props.put("plugin.name", "pgoutput");
        props.put("slot.name", "dummy_slot");
        return props;
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

    public TopicNamingStrategy<TableId> getTopicNamingStrategy() {
        return topicNamingStrategy;
    }
}
