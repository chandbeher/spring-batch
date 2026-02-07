package com.example.batch.dialect;

import com.example.batch.model.CanonicalColumn;
import java.util.List;
import java.util.Set;

public interface DatabaseDialect {
    String createTableSql(String table, List<CanonicalColumn> cols, Set<String> primaryKeys);

    List<String> alterTableSql(
        String table,
        List<CanonicalColumn> csvSchema,
        List<CanonicalColumn> dbSchema
    );

    String upsertSql(String table, Set<String> columns, Set<String> primaryKeys);

    String getColumnTypeSql(CanonicalColumn col);
}
