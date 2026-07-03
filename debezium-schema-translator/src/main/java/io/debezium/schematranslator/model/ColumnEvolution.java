package io.debezium.schematranslator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a single column-level change between two incompatible schemas.
 *
 * <p>Each change carries both a Postgres-style {@code label} (e.g. "nullable timestamp with
 * time zone") and the concise Avro-level {@code avro} form (e.g. {@code ["null", ZonedTimestamp]}),
 * so consumers can render a meaningful diff without re-parsing the raw Avro schemas. A column that
 * is a likely cause of the incompatibility also carries a {@code reason} explaining why.
 */
public class ColumnEvolution {

    /** Kind of change applied to a column. */
    public enum ChangeType {
        MODIFIED("modified"),
        ADDED("added"),
        REMOVED("removed");

        private final String value;

        ChangeType(String value) {
            this.value = value;
        }

        @JsonProperty
        public String getValue() {
            return value;
        }
    }

    @JsonProperty("column")
    private final String column;

    @JsonProperty("change")
    private final String change;

    @JsonProperty("old")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final TypeRef oldType;

    @JsonProperty("new")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final TypeRef newType;

    /** Why this column breaks Avro backward compatibility; {@code null} when it is not the cause. */
    @JsonProperty("reason")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String reason;

    public ColumnEvolution(String column, ChangeType change, TypeRef oldType, TypeRef newType) {
        this(column, change, oldType, newType, null);
    }

    public ColumnEvolution(String column, ChangeType change, TypeRef oldType, TypeRef newType, String reason) {
        this.column = column;
        this.change = change.getValue();
        this.oldType = oldType;
        this.newType = newType;
        this.reason = reason;
    }

    public String getColumn() {
        return column;
    }

    public String getChange() {
        return change;
    }

    public TypeRef getOldType() {
        return oldType;
    }

    public TypeRef getNewType() {
        return newType;
    }

    public String getReason() {
        return reason;
    }

    /**
     * A column type rendered two ways: a human-readable Postgres-style {@code label} and the
     * concise Avro-level {@code avro} form.
     */
    public static class TypeRef {

        @JsonProperty("label")
        private final String label;

        @JsonProperty("avro")
        private final String avro;

        public TypeRef(String label, String avro) {
            this.label = label;
            this.avro = avro;
        }

        public String getLabel() {
            return label;
        }

        public String getAvro() {
            return avro;
        }
    }
}
