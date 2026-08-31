# Debezium Schema Translator

A lightweight HTTP service that reads Postgres table schemas and registers them as Avro schemas with a Confluent Schema Registry.

It leverages Debezium's Postgres connector internals to extract schema information without performing any change data capture.

See the [ADR](https://docs.google.com/document/d/12jFNBuNmhXG2sPwgH7Hp9410fvSrbp4UudctiEhPLS8/edit?tab=t.0#heading=h.4ocygg1bsnsl) for the full design rationale and context.

## How it works

For each requested table, the service:
1. Reads the table schema from Postgres using the Debezium Postgres connector
2. Converts the Kafka Connect schema to Avro format
3. Registers both a value (envelope) schema and a key schema with the Schema Registry
4. Returns the schema IDs and versions

Subject names follow Debezium's topic naming convention:
- Value: `{TOPIC_PREFIX}.{schema}.{table}-value`
- Key: `{TOPIC_PREFIX}.{schema}.{table}-key`

## Configuration

Service-level configuration is done via environment variables. Postgres connection details are
not part of the service config — they are supplied per request via the `connection_string` field
in the request body.

| Variable              | Default                    | Required | Description                                          |
|-----------------------|----------------------------|----------|------------------------------------------------------|
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081`    |          | Confluent Schema Registry URL                        |
| `TOPIC_PREFIX`        |                            | Yes      | Debezium topic prefix, used to derive subject names  |
| `HTTP_PORT`           | `8080`                     |          | Port the HTTP server listens on                      |

## API

### Health check

```
GET /api/v1/schema-translator/health
```

```json
{"status": "UP"}
```

### Register schemas

```
POST /api/v1/schema-translator/schemas
Content-Type: application/json
```

**Request body:**

```json
{
  "tables": ["public.users", "orders", "myschema.events"],
  "connection_string": "postgresql://user:password@host:5432/dbname"
}
```

Table names can be `schema.table` or just `table` (defaults to `public` schema).

The `connection_string` is a Postgres URL of the form
`postgresql://user:password@host:port/dbname[?sslmode=...]`. A new Postgres connection is opened
for every request and closed before responding, so each call can target a different database.
When omitted, the SSL mode defaults to `prefer`.

**Success response (200):**

```json
{
  "registered_schemas": [
    {
      "table": "public.users",
      "subject": "test.public.users-value",
      "schema_id": 1,
      "version": 1
    },
    {
      "table": "public.users",
      "subject": "test.public.users-key",
      "schema_id": 2,
      "version": 1
    }
  ]
}
```

**Error responses:**

| Code | Cause                                                              |
|------|--------------------------------------------------------------------|
| 400  | Invalid request body, missing `tables`, or missing/malformed `connection_string` |
| 405  | Wrong HTTP method                                                  |
| 409  | Schema incompatible with an already-registered version             |
| 500  | Postgres read error or Schema Registry error                       |

Registration is idempotent: registering an identical schema multiple times returns the same schema ID and version.

### Delete schemas

```
DELETE /api/v1/schema-translator/schemas
```

Deletes the `-value` and `-key` subjects from the Schema Registry. The request body is optional.

**Without a body (or without a `tables` field)** every subject in the registry is deleted.

**With a body**, only the listed tables are deleted:

```json
{
  "tables": ["public.users", "orders"]
}
```

Table names follow the same rules as registration (`schema.table`, or `table` for the `public`
schema). Both subjects of a requested table are assumed to be registered: a table that was never
registered, or one without a primary key and therefore without a key subject, fails with a 500 from
the Schema Registry.

**Success response (200):**

```json
{
  "deleted_schemas": [
    {
      "table": "public.users",
      "subject": "test.public.users-value",
      "schema_id": 1,
      "version": 1
    }
  ],
  "deleted_count": 1
}
```

**Error responses:**

| Code | Cause                                                                  |
|------|------------------------------------------------------------------------|
| 400  | Malformed JSON body, empty `tables` array, or an unparseable table name |
| 405  | Wrong HTTP method                                                      |
| 500  | Schema Registry error, including a requested subject that is not registered |

## Build

```bash
mvn verify -pl debezium-schema-translator
```

This runs all tests and produces a shaded (fat) JAR at `target/debezium-schema-translator-*-shaded.jar`.

Integration tests use [Testcontainers](https://testcontainers.com/) and require Docker. To skip them:

```bash
mvn verify -pl debezium-schema-translator -DskipITs
```

## Local development

A `docker-compose.yml` is provided that starts Postgres, Kafka, Confluent Schema Registry, and the schema translator itself.

```bash
# Build the JAR first
mvn verify -pl debezium-schema-translator -DskipITs

# Start all services
docker-compose -f debezium-schema-translator/docker-compose.yml up -d

# Check health
curl -s http://localhost:8080/api/v1/schema-translator/health

# Create a table in Postgres, then register its schema
# (replace "public.my_table" with an actual table in your database)
curl -s -X POST http://localhost:8080/api/v1/schema-translator/schemas \
  -H 'Content-Type: application/json' \
  -d '{
        "tables": ["public.my_table"],
        "connection_string": "postgresql://postgres:postgres@postgres:5432/testdb"
      }'

# Inspect registered subjects in Schema Registry
curl -s http://localhost:8081/subjects

# Delete the schemas of a single table (omit the body to delete every subject)
curl -s -X DELETE http://localhost:8080/api/v1/schema-translator/schemas \
  -H 'Content-Type: application/json' \
  -d '{"tables": ["public.my_table"]}'
```