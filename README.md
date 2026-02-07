# Dynamic CSV Ingestion Framework (Spring Batch)

A production-grade, DB-agnostic ingestion framework built with Spring Batch.

## Features
- **Dynamic Schema Discovery**: Infers column types and lengths from CSV files.
- **Schema Drift Management**: Automatically creates tables or adds missing columns.
- **DB-Independent**: Supports multiple dialects (PostgreSQL, H2 included).
- **Production Ready**: Optimized for memory (chunking) and CPU (batching).
- **SQL Injection Protected**: Validates all dynamic identifiers.
- **Restartable**: Leverages Spring Batch's robust state management.

## How to Run
Run the job with the following parameters:
- `filePath`: Path to the CSV file.
- `tableName`: Target database table name.
- `primaryKeys` (Optional): Comma-separated list of primary keys for UPSERT.
- `sampleSize` (Optional): Number of rows to sample for schema discovery (default 100).

Example:
```bash
mvn spring-boot:run -Dspring-boot.run.arguments="filePath=/path/to/data.csv tableName=my_table primaryKeys=id"
```

## Architecture
See `docs/Db-independent Dynamic Csv Ingestion Framework (spring Batch).pdf` for detailed design.