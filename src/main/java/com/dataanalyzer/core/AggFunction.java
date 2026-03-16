package com.dataanalyzer.core;

/**
 * Supported aggregation functions for {@link DataAggregator}.
 */
public enum AggFunction {

    AVG("Média (avg)"),
    SUM("Soma (sum)"),
    MIN("Mínimo (min)"),
    MAX("Máximo (max)"),
    COUNT("Contagem (count)");

    private final String label;

    AggFunction(String label) {
        this.label = label;
    }

    /** @return human-readable label for display in menus */
    public String getLabel() {
        return label;
    }
}
