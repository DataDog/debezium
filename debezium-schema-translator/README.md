# Debezium Schema Translator

A lightweight HTTP service that reads PostgreSQL table schemas and registers them as Avro schemas with a Confluent Schema Registry.

It leverages Debezium's PostgreSQL connector internals to extract schema information without performing any change data capture.

See the [ADR](https://docs.google.com/document/d/12jFNBuNmhXG2sPwgH7Hp9410fvSrbp4UudctiEhPLS8/edit?tab=t.0#heading=h.4ocygg1bsnsl) for the full design rationale and context.

## How it works

For each requested table, the service:
1. Reads the table schema from PostgreSQL using the Debezium PostgreSQL connector
2. Converts the Kafka Connect schema to Avro format
3. Registers both a value (envelope) schema and a key schema with the Schema Registry
4. Returns the schema IDs and versions

Subject names follow Debezium's topic naming convention:
- Value: `{TOPIC_PREFIX}.{schema}.{table}-value`
- Key: `{TOPIC_PREFIX}.{schema}.{table}-key`

## Configuration

All configuration is done via environment variables.

| Variable              | Default                    | Required | Description                                          |
|-----------------------|----------------------------|----------|------------------------------------------------------|
| `POSTGRES_HOST`       | `localhost`                |          | PostgreSQL hostname                                  |
| `POSTGRES_PORT`       | `5432`                     |          | PostgreSQL port                                      |
| `POSTGRES_DATABASE`   |                            | Yes      | PostgreSQL database name                             |
| `POSTGRES_USER`       | `postgres`                 |          | PostgreSQL user                                      |
| `POSTGRES_PASSWORD`   |                            | Yes      | PostgreSQL password                                  |
| `POSTGRES_SSL_MODE`   | `prefer`                   |          | SSL mode (`disable`, `prefer`, `require`, etc.)      |
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
POST /api/v1/schema-translator/register-schemas
Content-Type: application/json
```

**Request body:**

```json
{
  "tables": ["public.users", "orders", "myschema.events"]
}
```

Table names can be `schema.table` or just `table` (defaults to `public` schema).

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

| Code | Cause                                                     |
|------|-----------------------------------------------------------|
| 400  | Invalid request body or missing `tables` field            |
| 405  | Wrong HTTP method                                         |
| 409  | Schema incompatible with an already-registered version    |
| 500  | PostgreSQL read error or Schema Registry error            |

Registration is idempotent: registering an identical schema multiple times returns the same schema ID and version.

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

A `docker-compose.yml` is provided that starts PostgreSQL, Kafka, Confluent Schema Registry, and the schema translator itself.

```bash
# Build the JAR first
mvn verify -pl debezium-schema-translator -DskipITs

# Start all services
docker-compose -f debezium-schema-translator/docker-compose.yml up -d

# Check health
curl -s http://localhost:8080/api/v1/schema-translator/health

# Create a table in PostgreSQL, then register its schema
# (replace "public.my_table" with an actual table in your database)
curl -s -X POST http://localhost:8080/api/v1/schema-translator/register-schemas \
  -H 'Content-Type: application/json' \
  -d '{"tables": ["public.my_table"]}'

# Inspect registered subjects in Schema Registry
curl -s http://localhost:8081/subjects
```