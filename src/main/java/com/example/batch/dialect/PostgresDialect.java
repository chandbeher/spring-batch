package com.example.batch.dialect;

import com.example.batch.model.CanonicalColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PostgresDialect implements DatabaseDialect {

    @Override
    public String createTableSql(String table, List<CanonicalColumn> cols, Set<String> primaryKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(table).append(" (\n");
        List<String> colDefs = cols.stream()
                .map(col -> col.getName() + " " + getColumnTypeSql(col) + (col.isNullable() ? "" : " NOT NULL"))
                .collect(Collectors.toList());
        sb.append(String.join(",\n", colDefs));
        if (!primaryKeys.isEmpty()) {
            sb.append(",\nPRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public List<String> alterTableSql(String table, List<CanonicalColumn> csvSchema, List<CanonicalColumn> dbSchema) {
        List<String> sqls = new ArrayList<>();
        Set<String> dbColNames = dbSchema.stream()
                .map(c -> c.getName().toLowerCase())
                .collect(Collectors.toSet());

        for (CanonicalColumn csvCol : csvSchema) {
            if (!dbColNames.contains(csvCol.getName().toLowerCase())) {
                sqls.add("ALTER TABLE " + table + " ADD COLUMN " + csvCol.getName() + " " + getColumnTypeSql(csvCol));
            }
        }
        return sqls;
    }

    @Override
    public String upsertSql(String table, Set<String> columns, Set<String> primaryKeys) {
        if (primaryKeys.isEmpty()) {
            // Fallback to simple INSERT if no primary keys defined for conflict
            String cols = String.join(", ", columns);
            String placeholders = columns.stream().map(c -> ":" + c).collect(Collectors.joining(", "));
            return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
        }

        String cols = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> ":" + c).collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(table).append(" (").append(cols).append(")\n");
        sb.append("VALUES (").append(placeholders).append(")\n");
        sb.append("ON CONFLICT (").append(String.join(", ", primaryKeys)).append(")\n");
        sb.append("DO UPDATE SET\n");

        String updateSet = columns.stream()
                .filter(c -> !primaryKeys.contains(c))
                .map(c -> c + " = EXCLUDED." + c)
                .collect(Collectors.joining(",\n"));

        if (updateSet.isEmpty()) {
            sb.append("NOTHING");
        } else {
            sb.append(updateSet);
        }

        return sb.toString();
    }

    @Override
    public String getColumnTypeSql(CanonicalColumn col) {
        return switch (col.getType()) {
            case STRING -> "VARCHAR(" + (col.getLength() != null ? col.getLength() : 255) + ")";
            case INTEGER -> "INTEGER";
            case LONG -> "BIGINT";
            case DECIMAL -> "DECIMAL(" + (col.getPrecision() != null ? col.getPrecision() : 19) + "," + (col.getScale() != null ? col.getScale() : 4) + ")";
            case BOOLEAN -> "BOOLEAN";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP";
        };
    }
}
