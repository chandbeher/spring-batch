package com.example.batch.writer;

import com.example.batch.dialect.DatabaseDialect;
import com.example.batch.model.CanonicalColumn;
import com.example.batch.util.SqlIdentifierValidator;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DynamicJdbcBatchItemWriter implements ItemWriter<Map<String, Object>> {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DatabaseDialect dialect;
    private final String tableName;
    private final List<CanonicalColumn> schema;
    private final Set<String> primaryKeys;
    private String sql;

    public DynamicJdbcBatchItemWriter(DataSource dataSource, DatabaseDialect dialect, String tableName, List<CanonicalColumn> schema, Set<String> primaryKeys) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.dialect = dialect;
        this.tableName = tableName;
        this.schema = schema;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
        if (sql == null) {
            SqlIdentifierValidator.validate(tableName);
            schema.forEach(col -> SqlIdentifierValidator.validate(col.getName()));
            primaryKeys.forEach(SqlIdentifierValidator::validate);

            Set<String> columns = schema.stream().map(CanonicalColumn::getName).collect(Collectors.toSet());
            sql = dialect.upsertSql(tableName, columns, primaryKeys);
        }

        SqlParameterSource[] batch = chunk.getItems().stream()
                .map(MapSqlParameterSource::new)
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(sql, batch);
    }
}
