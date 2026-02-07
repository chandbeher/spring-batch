package com.example.batch.step;

import com.example.batch.model.CanonicalColumn;
import com.example.batch.model.CanonicalType;
import com.example.batch.util.SqlIdentifierValidator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SchemaDiscoveryTasklet implements Tasklet {

    private final int sampleSize = 100;

    public SchemaDiscoveryTasklet() {
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String filePath = (String) chunkContext.getStepContext().getJobParameters().get("filePath");
        Long sampleSizeParam = (Long) chunkContext.getStepContext().getJobParameters().get("sampleSize");
        int effectiveSampleSize = sampleSizeParam != null ? sampleSizeParam.intValue() : this.sampleSize;

        log.info("Starting schema discovery for file: {} with sample size: {}", filePath, effectiveSampleSize);
        Resource resource = new FileSystemResource(filePath);

        try (Reader reader = new InputStreamReader(resource.getInputStream());
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            List<String> headers = parser.getHeaderNames();
            headers.forEach(SqlIdentifierValidator::validate);
            Map<String, CanonicalType> typeMap = new HashMap<>();
            Map<String, Integer> lengthMap = new HashMap<>();

            int count = 0;
            for (CSVRecord record : parser) {
                if (count >= effectiveSampleSize) break;

                for (String header : headers) {
                    String value = record.get(header);
                    updateInferredType(header, value, typeMap, lengthMap);
                }
                count++;
            }

            List<CanonicalColumn> columns = new ArrayList<>();
            for (String header : headers) {
                columns.add(CanonicalColumn.builder()
                        .name(header)
                        .type(typeMap.getOrDefault(header, CanonicalType.STRING))
                        .length(lengthMap.getOrDefault(header, 255))
                        .nullable(true)
                        .build());
            }

            chunkContext.getStepContext().getStepExecution().getJobExecution()
                    .getExecutionContext().put("csvSchema", columns);

            log.info("Discovered {} columns", columns.size());
        }

        return RepeatStatus.FINISHED;
    }

    private void updateInferredType(String header, String value, Map<String, CanonicalType> typeMap, Map<String, Integer> lengthMap) {
        if (value == null || value.isEmpty()) return;

        int currentLength = value.length();
        lengthMap.merge(header, currentLength, Math::max);

        CanonicalType inferred = inferType(value);
        CanonicalType existing = typeMap.get(header);

        if (existing == null) {
            typeMap.put(header, inferred);
        } else {
            typeMap.put(header, reconcile(existing, inferred));
        }
    }

    private CanonicalType inferType(String value) {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return CanonicalType.BOOLEAN;
        }
        try {
            Long.parseLong(value);
            return CanonicalType.LONG;
        } catch (NumberFormatException ignored) {}

        try {
            new BigDecimal(value);
            return CanonicalType.DECIMAL;
        } catch (NumberFormatException ignored) {}

        // Simple regex for date-like things (YYYY-MM-DD)
        if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return CanonicalType.DATE;
        }
        if (value.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*")) {
            return CanonicalType.TIMESTAMP;
        }

        return CanonicalType.STRING;
    }

    private CanonicalType reconcile(CanonicalType t1, CanonicalType t2) {
        if (t1 == t2) return t1;
        if (t1 == CanonicalType.STRING || t2 == CanonicalType.STRING) return CanonicalType.STRING;
        if (t1 == CanonicalType.DECIMAL || t2 == CanonicalType.DECIMAL) return CanonicalType.DECIMAL;
        if (t1 == CanonicalType.LONG && t2 == CanonicalType.INTEGER) return CanonicalType.LONG;
        if (t1 == CanonicalType.INTEGER && t2 == CanonicalType.LONG) return CanonicalType.LONG;

        return CanonicalType.STRING; // Fallback
    }
}
