package com.dataanalyzer.core;

/**
 * Supported output formats for {@link DataExporter}.
 */
public enum ExportFormat {

    /** CSV format. */
    CSV("CSV"),

    /** Parquet format. */
    PARQUET("Parquet"),

    /** JSON format. */
    JSON("JSON");

    /** Human-readable label for display in menus. */
    private final String label;

    /**
     * @param lbl human-readable label
     */
    ExportFormat(final String lbl) {
        this.label = lbl;
    }

    /** @return human-readable label for display in menus */
    public String getLabel() {
        return label;
    }
}
