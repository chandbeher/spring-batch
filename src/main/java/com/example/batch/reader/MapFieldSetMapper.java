package com.example.batch.reader;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.util.HashMap;
import java.util.Map;

public class MapFieldSetMapper implements FieldSetMapper<Map<String, String>> {
    @Override
    public Map<String, String> mapFieldSet(FieldSet fieldSet) throws BindException {
        Map<String, String> map = new HashMap<>();
        String[] names = fieldSet.getNames();
        for (String name : names) {
            map.put(name, fieldSet.readString(name));
        }
        return map;
    }
}
