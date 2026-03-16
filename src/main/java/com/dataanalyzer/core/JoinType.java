package com.dataanalyzer.core;

/**
 * Enumeration of the supported Spark join strategies.
 *
 * <p>Each constant carries the lowercase join-type string that Spark
 * accepts in {@code Dataset.join(other, condition, joinType)}.
 */
public enum JoinType {

    /** Inner join. */
    INNER("inner"),

    /** Left outer join. */
    LEFT("left"),

    /** Right outer join. */
    RIGHT("right"),

    /** Full outer join. */
    FULL("full");

    /** Spark join-type name. */
    private final String sparkName;

    /**
     * @param name Spark join-type string
     */
    JoinType(final String name) {
        this.sparkName = name;
    }

    /**
     * Returns the Spark join-type string recognised by the Dataset API.
     *
     * @return lowercase join-type name
     *         (e.g. {@code "inner"}, {@code "left"})
     */
    public String getSparkName() {
        return sparkName;
    }
}
