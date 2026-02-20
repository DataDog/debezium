/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schematranslator.schema;

import io.confluent.connect.avro.AvroData;

/**
 * Converts Kafka Connect {@link org.apache.kafka.connect.data.Schema} objects to
 * Apache Avro {@link org.apache.avro.Schema} using Confluent's {@link AvroData}.
 */
public class AvroSchemaConverter {

    private final AvroData avroData;

    public AvroSchemaConverter() {
        // Cache size of 1000 schema conversions
        this.avroData = new AvroData(1000);
    }

    /**
     * Converts a Kafka Connect schema to an Avro schema.
     */
    public org.apache.avro.Schema toAvro(org.apache.kafka.connect.data.Schema connectSchema) {
        return avroData.fromConnectSchema(connectSchema);
    }
}
