package io.debezium.schematranslator.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.schematranslator.model.ColumnEvolution;
import io.debezium.schematranslator.model.ColumnEvolution.TypeRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes a column-level diff between two incompatible Debezium Envelope Avro schemas.
 *
 * <p>Both schemas are the JSON produced by the Schema Registry / Avro. The analyzer walks the
 * {@code before} field down to its {@code Value} record and compares each column's type, emitting
 * {@link ColumnEvolution} entries describing what changed. Each change carries both a Postgres-style
 * label (e.g. "nullable timestamp with time zone") and the concise Avro-level form (e.g.
 * {@code ["null", ZonedTimestamp]}) so consumers do not need to re-parse the raw schemas.
 */
public class AvroSchemaDiffAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaDiffAnalyzer.class);

    /** Maps Debezium logical type names (Avro {@code connect.name}) to Postgres-style labels. */
    private static final Map<String, String> DEBEZIUM_TYPE_MAP = new LinkedHashMap<>();

    /** Maps Avro primitive type names to Postgres-style labels. */
    private static final Map<String, String> AVRO_PRIMITIVE_MAP = new LinkedHashMap<>();

    static {
        // Keys are sourced from Debezium's own logical-type name constants (rather than literal
        // strings) so they stay in lockstep with the library and fail to compile if a type is
        // renamed or removed. The Postgres-style label values, by contrast, must be authored here:
        // Debezium models the forward Postgres-type -> schema direction in the connector's type
        // registry (which needs a live column/OID), but exposes no reverse logical-type -> Postgres
        // label mapping, and at diff time we only have the Avro schema, not the source columns.
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.Uuid.LOGICAL_NAME, "uuid");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.Json.LOGICAL_NAME, "jsonb");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.Enum.LOGICAL_NAME, "enum");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.Bits.LOGICAL_NAME, "bit");
        DEBEZIUM_TYPE_MAP.put(io.debezium.connector.postgresql.data.Ltree.LOGICAL_NAME, "ltree");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.Xml.LOGICAL_NAME, "xml");
        // NUMERIC/DECIMAL and MONEY in the default precise mode (fixed scale -> Decimal, variable
        // scale -> VariableScaleDecimal). In double/string modes they serialize as plain double or
        // string and are covered by AVRO_PRIMITIVE_MAP instead.
        DEBEZIUM_TYPE_MAP.put(org.apache.kafka.connect.data.Decimal.LOGICAL_NAME, "numeric");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME, "numeric");
        // PostGIS types.
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.geometry.Geometry.LOGICAL_NAME, "geometry");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.geometry.Geography.LOGICAL_NAME, "geography");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.geometry.Point.LOGICAL_NAME, "point");
        // pgvector types.
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.vector.DoubleVector.LOGICAL_NAME, "vector");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.vector.FloatVector.LOGICAL_NAME, "halfvec");
        DEBEZIUM_TYPE_MAP.put(io.debezium.data.vector.SparseDoubleVector.LOGICAL_NAME, "sparsevec");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.ZonedTimestamp.SCHEMA_NAME, "timestamp with time zone");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.ZonedTime.SCHEMA_NAME, "time with time zone");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.Timestamp.SCHEMA_NAME, "timestamp");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.MicroTimestamp.SCHEMA_NAME, "timestamp (microseconds)");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.NanoTimestamp.SCHEMA_NAME, "timestamp (nanoseconds)");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.Date.SCHEMA_NAME, "date");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.Time.SCHEMA_NAME, "time");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.MicroTime.SCHEMA_NAME, "time (microseconds)");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.NanoTime.SCHEMA_NAME, "time (nanoseconds)");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.Interval.SCHEMA_NAME, "interval");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.MicroDuration.SCHEMA_NAME, "interval (microseconds)");
        // Temporal types under time.precision.mode=connect (Kafka Connect logical types). Timestamp
        // is registered before Time because "...data.Time" is a substring of "...data.Timestamp"
        // and the partial-name fallback below matches on containment.
        DEBEZIUM_TYPE_MAP.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, "timestamp");
        DEBEZIUM_TYPE_MAP.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, "date");
        DEBEZIUM_TYPE_MAP.put(org.apache.kafka.connect.data.Time.LOGICAL_NAME, "time");
        // Temporal types under time.precision.mode=isostring (ISO-8601 strings).
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.IsoTimestamp.SCHEMA_NAME, "timestamp");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.IsoDate.SCHEMA_NAME, "date");
        DEBEZIUM_TYPE_MAP.put(io.debezium.time.IsoTime.SCHEMA_NAME, "time");

        AVRO_PRIMITIVE_MAP.put("string", "text");
        AVRO_PRIMITIVE_MAP.put("int", "integer");
        AVRO_PRIMITIVE_MAP.put("long", "bigint");
        AVRO_PRIMITIVE_MAP.put("float", "real");
        AVRO_PRIMITIVE_MAP.put("double", "double precision");
        AVRO_PRIMITIVE_MAP.put("boolean", "boolean");
        AVRO_PRIMITIVE_MAP.put("bytes", "bytea");
        AVRO_PRIMITIVE_MAP.put("null", "null");
    }

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Compares the column-level fields between two Debezium Envelope schemas.
     *
     * @param oldSchemaJson the previously registered Avro schema (JSON)
     * @param newSchemaJson the incoming Avro schema (JSON)
     * @return the list of column changes; empty if either schema cannot be parsed
     */
    public List<ColumnEvolution> diff(String oldSchemaJson, String newSchemaJson) {
        List<ColumnEvolution> changes = new ArrayList<>();

        Map<String, JsonNode> oldFields;
        Map<String, JsonNode> newFields;
        try {
            oldFields = valueFields(objectMapper.readTree(oldSchemaJson));
            newFields = valueFields(objectMapper.readTree(newSchemaJson));
        }
        catch (Exception e) {
            LOGGER.warn("Could not parse schemas for column diff: {}", e.getMessage());
            return changes;
        }

        for (Map.Entry<String, JsonNode> entry : oldFields.entrySet()) {
            String column = entry.getKey();
            JsonNode oldType = entry.getValue();
            JsonNode newType = newFields.get(column);
            if (newType == null) {
                changes.add(new ColumnEvolution(column, ColumnEvolution.ChangeType.REMOVED,
                        new TypeRef(typeLabel(oldType), typeShort(oldType)), null));
                continue;
            }
            if (!oldType.equals(newType)) {
                String oldLabel = typeLabel(oldType);
                String newLabel = typeLabel(newType);
                if (!oldLabel.equals(newLabel)) {
                    changes.add(new ColumnEvolution(column, ColumnEvolution.ChangeType.MODIFIED,
                            new TypeRef(oldLabel, typeShort(oldType)),
                            new TypeRef(newLabel, typeShort(newType))));
                }
            }
        }

        for (Map.Entry<String, JsonNode> entry : newFields.entrySet()) {
            String column = entry.getKey();
            if (!oldFields.containsKey(column)) {
                JsonNode newType = entry.getValue();
                changes.add(new ColumnEvolution(column, ColumnEvolution.ChangeType.ADDED,
                        null, new TypeRef(typeLabel(newType), typeShort(newType))));
            }
        }

        return changes;
    }

    /**
     * Extracts the column name to type-node mapping from an Envelope schema's {@code before.Value}
     * record. Insertion order is preserved.
     */
    private static Map<String, JsonNode> valueFields(JsonNode envelope) {
        Map<String, JsonNode> fields = new LinkedHashMap<>();
        JsonNode envelopeFields = envelope.get("fields");
        if (envelopeFields == null || !envelopeFields.isArray()) {
            return fields;
        }
        for (JsonNode field : envelopeFields) {
            if (!"before".equals(text(field, "name"))) {
                continue;
            }
            JsonNode type = field.get("type");
            if (type == null || !type.isArray()) {
                continue;
            }
            for (JsonNode candidate : type) {
                if (candidate.isObject() && "Value".equals(text(candidate, "name"))) {
                    JsonNode valueFields = candidate.get("fields");
                    if (valueFields != null && valueFields.isArray()) {
                        for (JsonNode valueField : valueFields) {
                            String name = text(valueField, "name");
                            JsonNode fieldType = valueField.get("type");
                            if (name != null && fieldType != null) {
                                fields.put(name, fieldType);
                            }
                        }
                    }
                    return fields;
                }
            }
        }
        return fields;
    }

    /**
     * Returns a concise Avro-level label: the last segment of {@code connect.name}, the primitive
     * type name, or a bracketed union (e.g. {@code ["null", ZonedTimestamp]}).
     */
    static String typeShort(JsonNode type) {
        if (type.isArray()) {
            List<String> parts = new ArrayList<>();
            for (JsonNode element : type) {
                if (element.isTextual() && "null".equals(element.asText())) {
                    parts.add("\"null\"");
                }
                else {
                    parts.add(typeShort(element));
                }
            }
            return "[" + String.join(", ", parts) + "]";
        }
        if (type.isObject()) {
            String connectName = text(type, "connect.name");
            if (connectName != null && !connectName.isEmpty()) {
                int dot = connectName.lastIndexOf('.');
                return dot >= 0 ? connectName.substring(dot + 1) : connectName;
            }
            String raw = text(type, "type");
            return raw != null ? raw : "unknown";
        }
        return type.asText();
    }

    /**
     * Returns a short Postgres-style label for an Avro field type. Unions with {@code null} are
     * rendered as "nullable &lt;label&gt;".
     */
    static String typeLabel(JsonNode type) {
        if (type.isArray()) {
            JsonNode firstNonNull = null;
            for (JsonNode element : type) {
                if (!(element.isTextual() && "null".equals(element.asText()))) {
                    firstNonNull = element;
                    break;
                }
            }
            String label = firstNonNull != null ? typeLabel(firstNonNull) : "null";
            return "nullable " + label;
        }
        if (type.isObject()) {
            String connectName = text(type, "connect.name");
            if (connectName != null) {
                if (DEBEZIUM_TYPE_MAP.containsKey(connectName)) {
                    return DEBEZIUM_TYPE_MAP.get(connectName);
                }
                // Partial match for versioned or namespaced names not in the map.
                for (Map.Entry<String, String> entry : DEBEZIUM_TYPE_MAP.entrySet()) {
                    if (connectName.contains(entry.getKey())) {
                        return entry.getValue();
                    }
                }
            }
            String raw = text(type, "type");
            if (raw == null) {
                raw = "unknown";
            }
            return AVRO_PRIMITIVE_MAP.getOrDefault(raw, raw);
        }
        String raw = type.asText();
        return AVRO_PRIMITIVE_MAP.getOrDefault(raw, raw);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && value.isTextual() ? value.asText() : null;
    }
}
