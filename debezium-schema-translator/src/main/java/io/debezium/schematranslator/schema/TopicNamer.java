package io.debezium.schematranslator.schema;

import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Derives Debezium topic names, and the Schema Registry subjects built from them, for a table.
 *
 * <p>Both the register and the delete paths go through this class so a table always maps to the
 * same subjects, whichever side of the API is doing the work.
 */
public class TopicNamer {

    private static final String VALUE_SUFFIX = "-value";
    private static final String KEY_SUFFIX = "-key";

    private final TopicNamingStrategy<TableId> strategy;

    public TopicNamer(TopicNamingStrategy<TableId> strategy) {
        this.strategy = strategy;
    }

    /**
     * @param table table name, either {@code schema.table} or {@code table} (schema defaults to {@code public})
     * @throws IllegalArgumentException if the table name cannot be parsed
     */
    public String dataChangeTopic(String table) {
        return dataChangeTopic(parseTableId(table));
    }

    public String dataChangeTopic(TableId tableId) {
        return strategy.dataChangeTopic(tableId);
    }

    public String valueSubject(String table) {
        return dataChangeTopic(table) + VALUE_SUFFIX;
    }

    public String valueSubject(TableId tableId) {
        return dataChangeTopic(tableId) + VALUE_SUFFIX;
    }

    public String keySubject(String table) {
        return dataChangeTopic(table) + KEY_SUFFIX;
    }

    public String keySubject(TableId tableId) {
        return dataChangeTopic(tableId) + KEY_SUFFIX;
    }

    /** Package-private: only {@link DebeziumSchemaReader} needs the raw strategy. */
    TopicNamingStrategy<TableId> strategy() {
        return strategy;
    }

    /**
     * Parses a table name string into a {@link TableId}, defaulting the schema to {@code "public"}
     * when no schema is specified. Mirrors the logic of {@code PostgresSchema.parse()}.
     */
    public static TableId parseTableId(String table) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        return tableId.schema() == null
                ? new TableId(tableId.catalog(), "public", tableId.table())
                : tableId;
    }
}
