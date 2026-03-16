package com.dataanalyzer.core;

/**
 * Enumeration of Spark SQL type names supported for cast operations.
 *
 * <p>Each constant carries the lowercase type name that Spark recognises
 * in {@code col.cast(typeName)} and {@code CAST(col AS typeName)} SQL
 * expressions.
 */
public enum ColumnType {

    /** Integer type. */
    INT("int"),

    /** Long type. */
    LONG("long"),

    /** Double type. */
    DOUBLE("double"),

    /** Float type. */
    FLOAT("float"),

    /** String type. */
    STRING("string"),

    /** Boolean type. */
    BOOLEAN("boolean"),

    /** Date type. */
    DATE("date"),

    /** Timestamp type. */
    TIMESTAMP("timestamp");

    /** Spark SQL type name. */
    private final String sparkName;

    /**
     * @param name Spark SQL type name
     */
    ColumnType(final String name) {
        this.sparkName = name;
    }

    /**
     * Returns the Spark SQL type name for this constant.
     *
     * @return lowercase Spark type string
     *         (e.g. {@code "int"}, {@code "double"})
     */
    public String getSparkName() {
        return sparkName;
    }
}
