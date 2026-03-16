package com.dataanalyzer.core;

/**
 * Supported output formats for {@link DataExporter}.
 */
public enum ExportFormat {

    CSV("CSV"),
    PARQUET("Parquet"),
    JSON("JSON");

    private final String label;

    ExportFormat(String label) {
        this.label = label;
    }

    /** @return human-readable label for display in menus */
    public String getLabel() {
        return label;
    }
}
