package io.debezium.schematranslator.schema;

import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSchemaConverterTest {

    private AvroSchemaConverter converter;

    @BeforeEach
    void setUp() {
        converter = new AvroSchemaConverter();
    }

    @Test
    void convertsStructSchemaToAvroRecord() {
        Schema connectSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", SchemaBuilder.int32().build())
                .field("name", SchemaBuilder.string().optional().build())
                .build();

        org.apache.avro.Schema avroSchema = converter.toAvro(connectSchema);

        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getName()).isEqualTo("Value");
        assertThat(avroSchema.getNamespace()).isEqualTo("test");
        assertThat(avroSchema.getFields()).hasSize(2);
        assertThat(avroSchema.getField("id")).isNotNull();
        assertThat(avroSchema.getField("name")).isNotNull();
    }

    @Test
    void convertsNestedStructToNestedAvroRecord() {
        Schema inner = SchemaBuilder.struct()
                .name("test.Inner")
                .field("value", SchemaBuilder.int64().build())
                .build();
        Schema outer = SchemaBuilder.struct()
                .name("test.Outer")
                .field("nested", inner)
                .build();

        org.apache.avro.Schema avroSchema = converter.toAvro(outer);

        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        org.apache.avro.Schema.Field nestedField = avroSchema.getField("nested");
        assertThat(nestedField).isNotNull();
        assertThat(nestedField.schema().getType()).isEqualTo(Type.RECORD);
    }
}
