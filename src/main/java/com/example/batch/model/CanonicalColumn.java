package com.example.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CanonicalColumn implements Serializable {
    private String name;
    private CanonicalType type;
    private Integer length;
    private Integer precision;
    private Integer scale;
    private boolean nullable = true;
}
