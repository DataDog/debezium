/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.postgresql.core.BaseConnection;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;

public class PostgresWrappedDriverIT {

    @Test
    public void shouldStreamAndFlushThroughWrappedPgJdbcConnections() throws Exception {
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.JDBC_DRIVER, WrappingDriver.class.getName())
                .with(PostgresConnectorConfig.JDBC_URL, "jdbc:wrapped-postgresql://${hostname}:${port}/${dbname}")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "disabled")
                .with(PostgresConnectorConfig.REPLICATION_CONNECTION_PROXY, true)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SLOT_NAME, "wrapped_driver_test")
                .with(PostgresConnectorConfig.PUBLICATION_NAME, "wrapped_driver_test")
                .with(PostgresConnectorConfig.MAX_RETRIES, 0)
                .build());
        DebeziumOpenLineageEmitter.init(config.getConfig().asMap(), "postgresql");

        try (PostgresConnection sql = new PostgresConnection(config.getJdbcConfig(),
                registry -> PostgresValueConverter.of(config, java.nio.charset.StandardCharsets.UTF_8, registry),
                PostgresConnection.CONNECTION_GENERAL)) {
            assertThat(sql.connection()).isNotInstanceOf(BaseConnection.class);
            assertThat(sql.getTimestampUtils()).isNotNull();
            assertThat(sql.getTypeRegistry().get("int4")).isNotNull();
            sql.execute("CREATE TABLE wrapped_driver_test (id INTEGER PRIMARY KEY, value TEXT)");
            try {
                sql.execute("CREATE PUBLICATION wrapped_driver_test FOR TABLE wrapped_driver_test");
                sql.query("SELECT * FROM pg_create_logical_replication_slot('wrapped_driver_test', 'pgoutput')", rs -> assertThat(rs.next()).isTrue());
                try (ReplicationConnection replication = ReplicationConnection.builder(config)
                        .withPlugin(config.plugin())
                        .withSlot(config.slotName())
                        .withPublication(config.publicationName())
                        .withTypeRegistry(sql.getTypeRegistry())
                        .withSchema(TestHelper.getSchema(config, sql.getTypeRegistry()))
                        .statusUpdateInterval(Duration.ofMillis(100))
                        .jdbcMetadataConnection(sql)
                        .build()) {
                    try (ReplicationStream stream = replication.startStreaming(new WalPositionLocator())) {
                        sql.execute("INSERT INTO wrapped_driver_test VALUES (1, 'wrapped')");
                        AtomicBoolean committed = new AtomicBoolean();
                        AtomicBoolean inserted = new AtomicBoolean();
                        await().atMost(Duration.ofSeconds(10)).until(() -> {
                            stream.readPending(message -> {
                                if (message.getOperation() == ReplicationMessage.Operation.INSERT) {
                                    inserted.set(true);
                                }
                                if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                                    committed.set(true);
                                }
                            });
                            return committed.get() && inserted.get();
                        });
                        Lsn received = stream.lastReceivedLsn();
                        stream.flushLsn(received);
                        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                            SlotState slot = sql.getReplicationSlotState(config.slotName(), "pgoutput");
                            assertThat(slot.slotLastFlushedLsn().compareTo(received)).isGreaterThanOrEqualTo(0);
                        });
                    }
                }
                assertThat(sql.getReplicationSlotState(config.slotName(), "pgoutput")).isNotNull();
            }
            finally {
                sql.dropReplicationSlot(config.slotName());
                sql.dropPublication(config.publicationName());
                sql.execute("DROP TABLE wrapped_driver_test");
            }
        }
    }

    public static class WrappingDriver extends org.postgresql.Driver {
        @Override
        public Connection connect(String url, Properties properties) throws SQLException {
            Connection delegate = super.connect(url.replace("jdbc:wrapped-postgresql:", "jdbc:postgresql:"), properties);
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{ Connection.class },
                    (proxy, method, args) -> {
                        try {
                            return method.invoke(delegate, args);
                        }
                        catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }
    }
}
