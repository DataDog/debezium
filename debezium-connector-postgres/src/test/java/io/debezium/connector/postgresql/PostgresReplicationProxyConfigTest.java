/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.pgoutput.PgOutputMessageDecoder;

public class PostgresReplicationProxyConfigTest {

    @Test
    public void shouldPreserveDirectConnectionDefaults() {
        PostgresConnectorConfig config = new PostgresConnectorConfig(configuration().build());
        assertThat(config.isReplicationConnectionProxy()).isFalse();
        assertThat(config.dropSlotOnStop()).isFalse();
        assertThat(config.getJdbcConfig().getString("jdbc.driver", PostgresConnectorConfig.JDBC_DRIVER.defaultValueAsString()))
                .isEqualTo("org.postgresql.Driver");
        assertThat(options(config)).containsEntry("messages", "true");
    }

    @Test
    public void shouldAllowPreProvisionedProxyConfiguration() {
        PostgresConnectorConfig config = new PostgresConnectorConfig(proxyConfiguration().build());
        assertThat(config.isReplicationConnectionProxy()).isTrue();
        assertThat(options(config)).containsEntry("proto_version", "1").containsEntry("publication_names", "dbz_publication")
                .doesNotContainKey("messages");
        assertThat(config.validate().get(PostgresConnectorConfig.REPLICATION_CONNECTION_PROXY.name()).errorMessages()).isEmpty();
    }

    @Test
    public void shouldRejectIncompatibleProxySettings() {
        assertRejected(PostgresConnectorConfig.PLUGIN_NAME, "decoderbufs");
        assertRejected(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "all_tables");
        assertRejected(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered");
        assertRejected(PostgresConnectorConfig.DROP_SLOT_ON_STOP, "true");
        assertRejected(PostgresConnectorConfig.CREATE_FAIL_OVER_SLOT, "true");
        assertRejected(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET, "true");
        assertRejected(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "public.test:FULL");
        assertRejected(PostgresConnectorConfig.STREAM_PARAMS, "messages=true");
    }

    private static void assertRejected(Field field, String value) {
        Configuration config = proxyConfiguration().with(field, value).build();
        assertThat(config.validate(PostgresConnectorConfig.ALL_FIELDS)
                .get(PostgresConnectorConfig.REPLICATION_CONNECTION_PROXY.name()).errorMessages()).isNotEmpty();
        assertThatThrownBy(() -> new PostgresConnectorConfig(config)).isInstanceOf(ConfigException.class)
                .hasMessageContaining("Replication proxy mode requires");
    }

    private static Properties options(PostgresConnectorConfig config) {
        Properties options = new Properties();
        ChainedLogicalStreamBuilder builder = (ChainedLogicalStreamBuilder) Proxy.newProxyInstance(
                ChainedLogicalStreamBuilder.class.getClassLoader(), new Class<?>[]{ ChainedLogicalStreamBuilder.class },
                (proxy, method, args) -> {
                    if (!method.getName().equals("withSlotOption")) {
                        throw new AssertionError(method.getName());
                    }
                    options.setProperty((String) args[0], args[1].toString());
                    return proxy;
                });
        new PgOutputMessageDecoder(new MessageDecoderContext(config, null), null).defaultOptions(builder, version -> true);
        return options;
    }

    private static Configuration.Builder configuration() {
        return Configuration.create().with("topic.prefix", "test")
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.USER, "cdc")
                .with(PostgresConnectorConfig.DATABASE_NAME, "tenant");
    }

    private static Configuration.Builder proxyConfiguration() {
        return configuration().with(PostgresConnectorConfig.REPLICATION_CONNECTION_PROXY, true)
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "disabled");
    }
}
