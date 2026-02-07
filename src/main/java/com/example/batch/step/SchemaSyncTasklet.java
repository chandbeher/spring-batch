package com.example.batch.step;

import com.example.batch.dialect.DatabaseDialect;
import com.example.batch.model.CanonicalColumn;
import com.example.batch.model.CanonicalType;
import com.example.batch.util.SqlIdentifierValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SchemaSyncTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;
    private final DatabaseDialect dialect;
    private String tableName;

    public SchemaSyncTasklet(DataSource dataSource, DatabaseDialect dialect) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.dialect = dialect;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        this.tableName = (String) chunkContext.getStepContext().getJobParameters().get("tableName");
        SqlIdentifierValidator.validate(tableName);

        String pksParam = (String) chunkContext.getStepContext().getJobParameters().get("primaryKeys");
        Set<String> primaryKeys = StringUtils.hasText(pksParam)
            ? Arrays.stream(pksParam.split(",")).map(String::trim).collect(Collectors.toSet())
            : Collections.emptySet();
        primaryKeys.forEach(SqlIdentifierValidator::validate);

        List<CanonicalColumn> csvSchema = (List<CanonicalColumn>) chunkContext.getStepContext().getJobExecutionContext().get("csvSchema");
        if (csvSchema == null || csvSchema.isEmpty()) {
            throw new IllegalStateException("CSV schema not found in ExecutionContext");
        }
        csvSchema.forEach(col -> SqlIdentifierValidator.validate(col.getName()));

        if (!tableExists()) {
            log.info("Table {} does not exist. Creating it.", tableName);
            String createSql = dialect.createTableSql(tableName, csvSchema, primaryKeys);
            jdbcTemplate.execute(createSql);
        } else {
            log.info("Table {} exists. Checking for schema drift.", tableName);
            List<CanonicalColumn> dbSchema = getDbSchema();
            List<String> alterSqls = dialect.alterTableSql(tableName, csvSchema, dbSchema);
            for (String sql : alterSqls) {
                log.info("Executing DDL: {}", sql);
                jdbcTemplate.execute(sql);
            }
        }

        return RepeatStatus.FINISHED;
    }

    private boolean tableExists() throws SQLException {
        try (var conn = jdbcTemplate.getDataSource().getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String catalog = conn.getCatalog();
            String schema = getSchema(conn);
            try (ResultSet rs = metaData.getTables(catalog, schema, tableName.toUpperCase(), new String[]{"TABLE"})) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = metaData.getTables(catalog, schema, tableName.toLowerCase(), new String[]{"TABLE"})) {
                if (rs.next()) return true;
            }
            return false;
        }
    }

    private List<CanonicalColumn> getDbSchema() throws SQLException {
        List<CanonicalColumn> columns = new ArrayList<>();
        try (var conn = jdbcTemplate.getDataSource().getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String catalog = conn.getCatalog();
            String schema = getSchema(conn);
            try (ResultSet rs = metaData.getColumns(catalog, schema, tableName.toUpperCase(), null)) {
                while (rs.next()) {
                    columns.add(mapColumn(rs));
                }
            }
            if (columns.isEmpty()) {
                try (ResultSet rs = metaData.getColumns(catalog, schema, tableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        columns.add(mapColumn(rs));
                    }
                }
            }
        }
        return columns;
    }

    private String getSchema(java.sql.Connection conn) throws SQLException {
        try {
            return conn.getSchema();
        } catch (AbstractMethodError | Exception e) {
            return null;
        }
    }

    private CanonicalColumn mapColumn(ResultSet rs) throws SQLException {
        return CanonicalColumn.builder()
                .name(rs.getString("COLUMN_NAME"))
                .type(mapSqlType(rs.getInt("DATA_TYPE")))
                .length(rs.getInt("COLUMN_SIZE"))
                .nullable(rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable)
                .build();
    }

    private CanonicalType mapSqlType(int sqlType) {
        // Simple mapping for demonstration
        return switch (sqlType) {
            case java.sql.Types.INTEGER -> CanonicalType.INTEGER;
            case java.sql.Types.BIGINT -> CanonicalType.LONG;
            case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> CanonicalType.DECIMAL;
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> CanonicalType.BOOLEAN;
            case java.sql.Types.DATE -> CanonicalType.DATE;
            case java.sql.Types.TIMESTAMP -> CanonicalType.TIMESTAMP;
            default -> CanonicalType.STRING;
        };
    }
}
