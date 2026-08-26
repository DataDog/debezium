package io.debezium.schematranslator.schema;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicNamerTest {

    private final TopicNamer namer = new DebeziumSchemaReader("test").getTopicNamer();

    @Test
    void qualifiedTableNameMapsToSubjects() {
        assertThat(namer.valueSubject("public.users")).isEqualTo("test.public.users-value");
        assertThat(namer.keySubject("public.users")).isEqualTo("test.public.users-key");
    }

    @Test
    void unqualifiedTableNameDefaultsToPublicSchema() {
        assertThat(namer.valueSubject("users")).isEqualTo("test.public.users-value");
        assertThat(namer.keySubject("users")).isEqualTo("test.public.users-key");
    }

    @Test
    void nonPublicSchemaIsPreserved() {
        assertThat(namer.valueSubject("myschema.events")).isEqualTo("test.myschema.events-value");
    }

    @Test
    void tableIdAndStringFormsAgree() {
        TableId tableId = TopicNamer.parseTableId("myschema.events");
        assertThat(namer.valueSubject(tableId)).isEqualTo(namer.valueSubject("myschema.events"));
        assertThat(namer.keySubject(tableId)).isEqualTo(namer.keySubject("myschema.events"));
    }

    @Test
    void unparseableTableNameIsRejected() {
        assertThatThrownBy(() -> namer.valueSubject(""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
