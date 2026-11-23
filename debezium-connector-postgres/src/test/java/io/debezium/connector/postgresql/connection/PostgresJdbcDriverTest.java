/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.Encoding;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;

public class PostgresJdbcDriverTest {

    @Before
    public void before() {
        RecordingDriver.connections.clear();
        RecordingDriver.publicationExists = true;
        RecordingDriver.slotExists = true;
    }

    @Test
    public void shouldSelectDriverAndPreserveDriverProperties() throws Exception {
        JdbcConfiguration config = JdbcConfiguration.adapt(configuration().build().subset("database.", true));
        try (Connection ignored = PostgresConnection.FACTORY.connect(config)) {
            RecordedConnection connection = RecordingDriver.connections.get(0);
            assertThat(connection.url).isEqualTo("jdbc:wrapped://localhost:5432/tenant");
            assertThat(connection.properties).containsEntry("user", "cdc").containsEntry("password", "secret")
                    .containsEntry("clusterName", "tenant");
            assertThat(connection.properties).doesNotContainKeys("jdbc.driver", "jdbc.url");
        }
    }

    @Test
    public void shouldRejectDriverThatDoesNotAcceptUrl() {
        JdbcConfiguration config = JdbcConfiguration.adapt(configuration().with(PostgresConnectorConfig.JDBC_URL, "jdbc:other:tenant")
                .build().subset("database.", true));
        assertThatThrownBy(() -> PostgresConnection.FACTORY.connect(config))
                .isInstanceOf(SQLException.class).hasMessageContaining("does not accept");
    }

    @Test
    public void shouldUnwrapConnectionWithoutReplacingOwnedWrapper() throws Exception {
        JdbcConfiguration config = JdbcConfiguration.adapt(configuration().build().subset("database.", true));
        try (PostgresConnection connection = new PostgresConnection(config, PostgresConnection.CONNECTION_GENERAL)) {
            Connection wrapper = connection.connection();
            assertThat(wrapper).isNotInstanceOf(BaseConnection.class);
            assertThat(connection.pgConnection()).isSameAs(wrapper.unwrap(BaseConnection.class));
            assertThat(connection.connection()).isSameAs(wrapper);
            assertThat(connection.getDatabaseCharset().name()).isEqualTo("UTF-8");
            assertThat(connection.connectionString()).isEqualTo("jdbc:wrapped://localhost:5432/tenant");
        }
        assertThat(RecordingDriver.connections.get(0).closed).isTrue();
    }

    @Test
    public void shouldUseSeparateSqlAndReplicationConnections() throws Exception {
        PostgresConnectorConfig config = new PostgresConnectorConfig(proxyConfiguration().build());
        try (PostgresConnection sql = new PostgresConnection(config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL);
                PostgresReplicationConnection replication = replication(config, sql)) {
            replication.initConnection();
            RecordedConnection streaming = RecordingDriver.connections.stream().filter(c -> c.replication).findFirst().orElseThrow();
            assertThat(streaming.properties).containsEntry("replication", "database")
                    .containsEntry("preferQueryMode", "simple").containsEntry("assumeMinServerVersion", "9.4");
            assertThat(streaming.queries).containsExactly("IDENTIFY_SYSTEM");
            assertThat(RecordingDriver.connections.stream().filter(c -> !c.replication).flatMap(c -> c.queries.stream()))
                    .anyMatch(query -> query.contains("pg_publication"))
                    .anyMatch(query -> query.contains("pg_replication_slots") && query.contains("current_database()"));
        }
        assertThat(RecordingDriver.connections).allMatch(c -> c.closed);
    }

    @Test
    public void shouldFailWithoutCreatingMissingPublication() throws Exception {
        RecordingDriver.publicationExists = false;
        PostgresConnectorConfig config = new PostgresConnectorConfig(proxyConfiguration().build());
        try (PostgresConnection sql = new PostgresConnection(config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL);
                PostgresReplicationConnection replication = replication(config, sql)) {
            assertThatThrownBy(replication::initConnection).isInstanceOf(ConnectException.class)
                    .hasMessageContaining("Publication").hasMessageContaining("must be provisioned");
        }
        assertThat(RecordingDriver.connections).noneMatch(c -> c.replication);
    }

    @Test
    public void shouldFailWithoutCreatingMissingSlot() throws Exception {
        RecordingDriver.slotExists = false;
        PostgresConnectorConfig config = new PostgresConnectorConfig(proxyConfiguration().build());
        try (PostgresConnection sql = new PostgresConnection(config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL);
                PostgresReplicationConnection replication = replication(config, sql)) {
            assertThatThrownBy(replication::initConnection).isInstanceOf(ConnectException.class)
                    .hasMessageContaining("Replication slot").hasMessageContaining("must be provisioned");
            assertThatThrownBy(replication::createReplicationSlot).isInstanceOf(ConnectException.class);
        }
        assertThat(RecordingDriver.connections).noneMatch(c -> c.replication);
    }

    private static PostgresReplicationConnection replication(PostgresConnectorConfig config, PostgresConnection sql) {
        return (PostgresReplicationConnection) new PostgresReplicationConnection.ReplicationConnectionBuilder(config)
                .withPlugin(config.plugin()).jdbcMetadataConnection(sql).build();
    }

    private static Configuration.Builder configuration() {
        return Configuration.create()
                .with("topic.prefix", "test")
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.DATABASE_NAME, "tenant")
                .with(PostgresConnectorConfig.USER, "cdc")
                .with(PostgresConnectorConfig.PASSWORD, "secret")
                .with(PostgresConnectorConfig.JDBC_DRIVER, RecordingDriver.class.getName())
                .with(PostgresConnectorConfig.JDBC_URL, "jdbc:wrapped://${hostname}:${port}/${dbname}")
                .with("database.clusterName", "tenant");
    }

    private static Configuration.Builder proxyConfiguration() {
        return configuration().with(PostgresConnectorConfig.REPLICATION_CONNECTION_PROXY, true)
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "disabled");
    }

    public static class RecordingDriver extends org.postgresql.Driver {
        private static final List<RecordedConnection> connections = new ArrayList<>();
        private static boolean publicationExists;
        private static boolean slotExists;

        @Override
        public Connection connect(String url, Properties properties) {
            if (!url.startsWith("jdbc:wrapped:")) {
                return null;
            }
            RecordedConnection connection = new RecordedConnection(url, properties);
            connections.add(connection);
            return connection.wrapper;
        }
    }

    private static class RecordedConnection {
        private final String url;
        private final Properties properties = new Properties();
        private final List<String> queries = new ArrayList<>();
        private final boolean replication;
        private final Connection wrapper;
        private boolean closed;

        private RecordedConnection(String url, Properties properties) {
            this.url = url;
            this.properties.putAll(properties);
            this.replication = "database".equals(properties.getProperty("replication"));
            BaseConnection pg = proxy(BaseConnection.class, (p, method, args) -> {
                if (method.getName().equals("getEncoding")) {
                    return Encoding.getJVMEncoding("UTF-8");
                }
                throw new AssertionError("Unexpected pgjdbc operation: " + method.getName());
            });
            wrapper = proxy(Connection.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "unwrap":
                        return ((Class<?>) args[0]).cast(pg);
                    case "isClosed":
                        return closed;
                    case "close":
                        closed = true;
                        return null;
                    case "getAutoCommit":
                        return true;
                    case "getMetaData":
                        return proxy(DatabaseMetaData.class, (m, operation, parameters) -> {
                            if (operation.getName().equals("getDatabaseMajorVersion")) {
                                return 17;
                            }
                            if (operation.getName().equals("getDatabaseMinorVersion")) {
                                return 0;
                            }
                            throw new AssertionError(operation.getName());
                        });
                    case "createStatement":
                        return statement(null);
                    case "prepareStatement":
                        return statement((String) args[0]);
                    default:
                        throw new AssertionError("Unexpected JDBC operation: " + method.getName());
                }
            });
        }

        private PreparedStatement statement(String preparedSql) {
            return proxy(PreparedStatement.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "getConnection":
                        return wrapper;
                    case "setQueryTimeout":
                    case "setString":
                    case "close":
                        return null;
                    case "executeQuery":
                        String sql = preparedSql == null ? (String) args[0] : preparedSql;
                        if (replication && !sql.equals("IDENTIFY_SYSTEM")) {
                            throw new AssertionError("SQL on replication connection: " + sql);
                        }
                        queries.add(sql);
                        AtomicBoolean available = new AtomicBoolean(sql.contains("pg_publication") ? RecordingDriver.publicationExists
                                : sql.contains("pg_replication_slots") ? RecordingDriver.slotExists : true);
                        return proxy(ResultSet.class, (r, operation, parameters) -> {
                            switch (operation.getName()) {
                                case "next":
                                    return available.getAndSet(false);
                                case "getString":
                                    return "xlogpos".equals(parameters[0]) ? "0/20" : "0/10";
                                case "getBoolean":
                                    return false;
                                case "getLong":
                                    return 0L;
                                case "close":
                                    return null;
                                default:
                                    throw new AssertionError(operation.getName());
                            }
                        });
                    default:
                        throw new AssertionError("Unexpected statement operation: " + method.getName());
                }
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{ type }, handler));
    }
}
