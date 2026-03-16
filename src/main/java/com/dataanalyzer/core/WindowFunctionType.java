package com.dataanalyzer.core;

/**
 * Enumeration of the Spark window functions supported by
 * {@link DataTransformer#applyWindowFunction}.
 */
public enum WindowFunctionType {

    /** Rank window function. */
    RANK("rank"),

    /** Dense rank window function. */
    DENSE_RANK("dense_rank"),

    /** Row number window function. */
    ROW_NUMBER("row_number"),

    /** Lag window function. */
    LAG("lag"),

    /** Lead window function. */
    LEAD("lead");

    /** Human-readable label and Spark function name. */
    private final String label;

    /**
     * @param lbl human-readable label / Spark function name
     */
    WindowFunctionType(final String lbl) {
        this.label = lbl;
    }

    /**
     * Returns the human-readable label / Spark function name.
     *
     * @return label string
     *         (e.g. {@code "rank"}, {@code "dense_rank"})
     */
    public String getLabel() {
        return label;
    }
}
