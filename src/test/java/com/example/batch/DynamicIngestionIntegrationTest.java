package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("test")
public class DynamicIngestionIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void testFullIngestionFlow() throws Exception {
        // 1. Initial ingestion
        Path csv1 = Files.createTempFile("data1", ".csv");
        Files.writeString(csv1, "id,name,age\n1,John,30\n2,Jane,25");

        JobParameters params1 = new JobParametersBuilder()
                .addString("filePath", csv1.toString())
                .addString("tableName", "users")
                .addString("primaryKeys", "id")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        var execution1 = jobLauncherTestUtils.launchJob(params1);
        assertThat(execution1.getExitStatus().getExitCode()).isEqualTo("COMPLETED");

        List<Map<String, Object>> results1 = jdbcTemplate.queryForList("SELECT * FROM users ORDER BY id");
        assertThat(results1).hasSize(2);
        assertThat(results1.get(0).get("NAME")).isEqualTo("John");
        assertThat(results1.get(0).get("AGE")).isEqualTo(30L); // H2 might return Long for INT depending on config

        // 2. Schema drift ingestion (add email column)
        Path csv2 = Files.createTempFile("data2", ".csv");
        Files.writeString(csv2, "id,name,age,email\n3,Bob,40,bob@example.com");

        JobParameters params2 = new JobParametersBuilder()
                .addString("filePath", csv2.toString())
                .addString("tableName", "users")
                .addString("primaryKeys", "id")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        var execution2 = jobLauncherTestUtils.launchJob(params2);
        assertThat(execution2.getExitStatus().getExitCode()).isEqualTo("COMPLETED");

        List<Map<String, Object>> results2 = jdbcTemplate.queryForList("SELECT * FROM users ORDER BY id");
        assertThat(results2).hasSize(3);
        assertThat(results2.get(2).get("EMAIL")).isEqualTo("bob@example.com");
    }

    @Test
    public void testUpsert() throws Exception {
        jdbcTemplate.execute("DROP TABLE IF EXISTS products");
        Path csv1 = Files.createTempFile("prod1", ".csv");
        Files.writeString(csv1, "sku,price\nSKU001,10.0\nSKU002,20.0");

        JobParameters params1 = new JobParametersBuilder()
                .addString("filePath", csv1.toString())
                .addString("tableName", "products")
                .addString("primaryKeys", "sku")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncherTestUtils.launchJob(params1);

        // Update SKU001 price and add SKU003
        Path csv2 = Files.createTempFile("prod2", ".csv");
        Files.writeString(csv2, "sku,price\nSKU001,15.0\nSKU003,30.0");

        JobParameters params2 = new JobParametersBuilder()
                .addString("filePath", csv2.toString())
                .addString("tableName", "products")
                .addString("primaryKeys", "sku")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncherTestUtils.launchJob(params2);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM products ORDER BY sku");
        assertThat(results).hasSize(3);
        assertThat(((java.math.BigDecimal)results.stream().filter(m -> m.get("SKU").equals("SKU001")).findFirst().get().get("PRICE")))
                .isEqualByComparingTo(new java.math.BigDecimal("15.0"));
    }

    @Test
    public void testDataTypes() throws Exception {
        Path csv = Files.createTempFile("types", ".csv");
        Files.writeString(csv, "id,is_active,salary,join_date\n101,true,50000.50,2023-01-01\n102,false,60000.00,2023-02-01");

        JobParameters params = new JobParametersBuilder()
                .addString("filePath", csv.toString())
                .addString("tableName", "employee_data")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        var execution = jobLauncherTestUtils.launchJob(params);
        assertThat(execution.getExitStatus().getExitCode()).isEqualTo("COMPLETED");

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM employee_data ORDER BY id");
        assertThat(results).hasSize(2);
        assertThat(results.get(0).get("IS_ACTIVE")).isEqualTo(true);
        assertThat(results.get(0).get("SALARY")).isInstanceOf(java.math.BigDecimal.class);
        assertThat(results.get(0).get("JOIN_DATE")).isInstanceOf(java.sql.Date.class);
    }

    @Test
    public void testSqlInjectionPrevention() throws Exception {
        Path csv = Files.createTempFile("injection", ".csv");
        Files.writeString(csv, "id,name\n1,John");

        JobParameters params = new JobParametersBuilder()
                .addString("filePath", csv.toString())
                .addString("tableName", "users; DROP TABLE users;")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();
        var execution = jobLauncherTestUtils.launchJob(params);
        assertThat(execution.getStatus().isUnsuccessful()).isTrue();
        assertThat(execution.getAllFailureExceptions().get(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(execution.getAllFailureExceptions().get(0).getMessage()).contains("Invalid SQL identifier");
    }
}
