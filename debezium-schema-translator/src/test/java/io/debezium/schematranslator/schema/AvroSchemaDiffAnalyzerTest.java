package io.debezium.schematranslator.schema;

import io.debezium.schematranslator.model.ColumnEvolution;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSchemaDiffAnalyzerTest {

    private final AvroSchemaDiffAnalyzer analyzer = new AvroSchemaDiffAnalyzer();

    /**
     * Wraps a {@code before.Value} field fragment into a full Debezium Envelope schema, the shape
     * the Schema Registry returns and that the analyzer walks.
     */
    private static String envelope(String valueFields) {
        return """
                {
                  "type": "record",
                  "name": "Envelope",
                  "namespace": "test.public.users",
                  "fields": [
                    {
                      "name": "before",
                      "type": ["null", {
                        "type": "record",
                        "name": "Value",
                        "fields": [%s]
                      }]
                    },
                    {"name": "op", "type": "string"}
                  ]
                }
                """.formatted(valueFields);
    }

    private static final String ZONED_TIMESTAMP =
            "{\"type\": \"string\", \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}";

    @Test
    void detectsNullabilityAndTypeNarrowing() {
        // 'ttl' goes from nullable timestamp-with-tz to non-nullable — the ticket's example.
        String oldSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, "
                        + "{\"name\": \"ttl\", \"type\": [\"null\", " + ZONED_TIMESTAMP + "]}");
        String newSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, "
                        + "{\"name\": \"ttl\", \"type\": " + ZONED_TIMESTAMP + "}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        ColumnEvolution ttl = changes.get(0);
        assertThat(ttl.getColumn()).isEqualTo("ttl");
        assertThat(ttl.getChange()).isEqualTo("modified");
        assertThat(ttl.getOldType().getLabel()).isEqualTo("nullable timestamp with time zone");
        assertThat(ttl.getOldType().getAvro()).isEqualTo("[\"null\", ZonedTimestamp]");
        assertThat(ttl.getNewType().getLabel()).isEqualTo("timestamp with time zone");
        assertThat(ttl.getNewType().getAvro()).isEqualTo("ZonedTimestamp");
    }

    @Test
    void detectsPrimitiveTypeChange() {
        String oldSchema = envelope("{\"name\": \"count\", \"type\": \"int\"}");
        String newSchema = envelope("{\"name\": \"count\", \"type\": \"long\"}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        ColumnEvolution count = changes.get(0);
        assertThat(count.getChange()).isEqualTo("modified");
        assertThat(count.getOldType().getLabel()).isEqualTo("integer");
        assertThat(count.getOldType().getAvro()).isEqualTo("int");
        assertThat(count.getNewType().getLabel()).isEqualTo("bigint");
        assertThat(count.getNewType().getAvro()).isEqualTo("long");
    }

    @Test
    void detectsAddedColumn() {
        String oldSchema = envelope("{\"name\": \"id\", \"type\": \"int\"}");
        String newSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"email\", \"type\": \"string\"}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        ColumnEvolution email = changes.get(0);
        assertThat(email.getColumn()).isEqualTo("email");
        assertThat(email.getChange()).isEqualTo("added");
        assertThat(email.getOldType()).isNull();
        assertThat(email.getNewType().getLabel()).isEqualTo("text");
        assertThat(email.getNewType().getAvro()).isEqualTo("string");
    }

    @Test
    void detectsRemovedColumn() {
        String oldSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"legacy\", \"type\": \"string\"}");
        String newSchema = envelope("{\"name\": \"id\", \"type\": \"int\"}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        ColumnEvolution legacy = changes.get(0);
        assertThat(legacy.getColumn()).isEqualTo("legacy");
        assertThat(legacy.getChange()).isEqualTo("removed");
        assertThat(legacy.getOldType().getLabel()).isEqualTo("text");
        assertThat(legacy.getNewType()).isNull();
    }

    @Test
    void mapsDebeziumLogicalType() {
        String oldSchema = envelope("{\"name\": \"id\", \"type\": \"int\"}");
        String newSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, "
                        + "{\"name\": \"uid\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.data.Uuid\"}}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getNewType().getLabel()).isEqualTo("uuid");
        assertThat(changes.get(0).getNewType().getAvro()).isEqualTo("Uuid");
    }

    @Test
    void mapsDecimalToNumericNotBytea() {
        // NUMERIC serializes as a Decimal logical type over Avro bytes; it must read as "numeric",
        // not fall through to the bytes -> "bytea" primitive mapping.
        String oldSchema = envelope("{\"name\": \"id\", \"type\": \"int\"}");
        String newSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, "
                        + "{\"name\": \"amount\", \"type\": {\"type\": \"bytes\", "
                        + "\"connect.name\": \"org.apache.kafka.connect.data.Decimal\"}}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getColumn()).isEqualTo("amount");
        assertThat(changes.get(0).getNewType().getLabel()).isEqualTo("numeric");
        assertThat(changes.get(0).getNewType().getAvro()).isEqualTo("Decimal");
    }

    @Test
    void mapsPostgisAndVectorTypes() {
        String oldSchema = envelope("{\"name\": \"id\", \"type\": \"int\"}");
        String newSchema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, "
                        + "{\"name\": \"geom\", \"type\": {\"type\": \"bytes\", \"connect.name\": \"io.debezium.data.geometry.Geometry\"}}, "
                        + "{\"name\": \"embedding\", \"type\": {\"type\": \"array\", \"connect.name\": \"io.debezium.data.DoubleVector\"}}");

        List<ColumnEvolution> changes = analyzer.diff(oldSchema, newSchema);

        assertThat(changes).extracting(c -> c.getColumn() + ":" + c.getNewType().getLabel())
                .containsExactlyInAnyOrder("geom:geometry", "embedding:vector");
    }

    @Test
    void ignoresIdenticalSchemas() {
        String schema = envelope(
                "{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}");

        assertThat(analyzer.diff(schema, schema)).isEmpty();
    }

    @Test
    void returnsEmptyOnMalformedJson() {
        assertThat(analyzer.diff("not-json", "also-not-json")).isEmpty();
        assertThat(analyzer.diff("could not retrieve existing schema: timeout",
                envelope("{\"name\": \"id\", \"type\": \"int\"}"))).isEmpty();
    }
}
