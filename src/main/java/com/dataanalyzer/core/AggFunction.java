package com.dataanalyzer.core;

/**
 * Supported aggregation functions for {@link DataAggregator}.
 */
public enum AggFunction {

    /** Average aggregation function. */
    AVG("Média (avg)"),

    /** Sum aggregation function. */
    SUM("Soma (sum)"),

    /** Minimum aggregation function. */
    MIN("Mínimo (min)"),

    /** Maximum aggregation function. */
    MAX("Máximo (max)"),

    /** Count aggregation function. */
    COUNT("Contagem (count)");

    /** Human-readable label for display in menus. */
    private final String label;

    /**
     * @param lbl human-readable label
     */
    AggFunction(final String lbl) {
        this.label = lbl;
    }

    /** @return human-readable label for display in menus */
    public String getLabel() {
        return label;
    }
}
