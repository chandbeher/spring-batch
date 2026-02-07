package com.example.batch.processor;

import com.example.batch.model.CanonicalColumn;
import com.example.batch.model.CanonicalType;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicTypeProcessor implements ItemProcessor<Map<String, String>, Map<String, Object>> {

    private final List<CanonicalColumn> schema;

    public DynamicTypeProcessor(List<CanonicalColumn> schema) {
        this.schema = schema;
    }

    @Override
    public Map<String, Object> process(Map<String, String> item) throws Exception {
        Map<String, Object> processed = new HashMap<>();
        for (CanonicalColumn col : schema) {
            String value = item.get(col.getName());
            processed.put(col.getName(), convert(value, col.getType()));
        }
        return processed;
    }

    private Object convert(String value, CanonicalType type) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return switch (type) {
            case INTEGER -> Integer.parseInt(value);
            case LONG -> Long.parseLong(value);
            case DECIMAL -> new BigDecimal(value);
            case BOOLEAN -> Boolean.parseBoolean(value);
            case DATE -> LocalDate.parse(value);
            case TIMESTAMP -> LocalDateTime.parse(value);
            case STRING -> value;
        };
    }
}
