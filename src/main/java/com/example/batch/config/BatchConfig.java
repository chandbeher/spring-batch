package com.example.batch.config;

import com.example.batch.dialect.DatabaseDialect;
import com.example.batch.dialect.H2Dialect;
import com.example.batch.dialect.PostgresDialect;
import com.example.batch.model.CanonicalColumn;
import com.example.batch.processor.DynamicTypeProcessor;
import com.example.batch.reader.MapFieldSetMapper;
import com.example.batch.step.SchemaDiscoveryTasklet;
import com.example.batch.step.SchemaSyncTasklet;
import com.example.batch.writer.DynamicJdbcBatchItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
public class BatchConfig {

    @Bean
    public DatabaseDialect databaseDialect(@Value("${app.database.type:h2}") String dbType) {
        return switch (dbType.toLowerCase()) {
            case "postgres", "postgresql" -> new PostgresDialect();
            default -> new H2Dialect();
        };
    }

    @Bean
    public Job dynamicCsvIngestionJob(JobRepository jobRepository,
                                      Step schemaDiscoveryStep,
                                      Step schemaSyncStep,
                                      Step dataLoadStep) {
        return new JobBuilder("dynamicCsvIngestionJob", jobRepository)
                .start(schemaDiscoveryStep)
                .next(schemaSyncStep)
                .next(dataLoadStep)
                .build();
    }

    @Bean
    public Step schemaDiscoveryStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("schemaDiscoveryStep", jobRepository)
                .tasklet(new SchemaDiscoveryTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Step schemaSyncStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                               DataSource dataSource, DatabaseDialect dialect) {
        return new StepBuilder("schemaSyncStep", jobRepository)
                .tasklet(new SchemaSyncTasklet(dataSource, dialect), transactionManager)
                .build();
    }

    @Bean
    public Step dataLoadStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             FlatFileItemReader<Map<String, String>> reader,
                             DynamicTypeProcessor processor,
                             DynamicJdbcBatchItemWriter writer) {
        return new StepBuilder("dataLoadStep", jobRepository)
                .<Map<String, String>, Map<String, Object>>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Map<String, String>> reader(@Value("#{jobParameters['filePath']}") String filePath,
                                                          @Value("#{jobExecutionContext['csvSchema']}") List<CanonicalColumn> columns) {
        String[] names = columns.stream().map(CanonicalColumn::getName).toArray(String[]::new);

        return new FlatFileItemReaderBuilder<Map<String, String>>()
                .name("csvReader")
                .resource(new FileSystemResource(filePath))
                .linesToSkip(1) // Skip header
                .delimited()
                .names(names)
                .fieldSetMapper(new MapFieldSetMapper())
                .build();
    }

    @Bean
    @StepScope
    public DynamicTypeProcessor processor(@Value("#{jobExecutionContext['csvSchema']}") List<CanonicalColumn> columns) {
        return new DynamicTypeProcessor(columns);
    }

    @Bean
    @StepScope
    public DynamicJdbcBatchItemWriter writer(DataSource dataSource, DatabaseDialect dialect,
                                             @Value("#{jobParameters['tableName']}") String tableName,
                                             @Value("#{jobParameters['primaryKeys']}") String pks,
                                             @Value("#{jobExecutionContext['csvSchema']}") List<CanonicalColumn> columns) {
        Set<String> primaryKeys = StringUtils.hasText(pks)
                ? Arrays.stream(pks.split(",")).map(String::trim).collect(Collectors.toSet())
                : Collections.emptySet();
        return new DynamicJdbcBatchItemWriter(dataSource, dialect, tableName, columns, primaryKeys);
    }
}
