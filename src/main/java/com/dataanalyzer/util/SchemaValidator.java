package com.dataanalyzer.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;

/**
 * Utility class with stateless helpers for DataFrame schema validation.
 */
public final class SchemaValidator {

    private SchemaValidator() { }

    /**
     * Returns true if the DataFrame contains a column with the given name.
     *
     * @param df         DataFrame to inspect
     * @param columnName column name to look up
     * @return {@code true} if the column exists
     */
    public static boolean columnExists(
            final Dataset<Row> df, final String columnName) {
        return Arrays.asList(df.columns()).contains(columnName);
    }

    /**
     * Returns true if all given column names exist in the DataFrame.
     *
     * @param df          DataFrame to inspect
     * @param columnNames column names to check
     * @return {@code true} if every column is present
     */
    public static boolean allColumnsExist(
            final Dataset<Row> df,
            final String... columnNames) {
        return Arrays.stream(columnNames)
            .allMatch(c -> columnExists(df, c));
    }

    /**
     * Returns true if the column holds a numeric data type.
     *
     * @param df         DataFrame to inspect
     * @param columnName column name to check
     * @return {@code true} if the column is numeric
     */
    public static boolean isNumeric(
            final Dataset<Row> df, final String columnName) {
        if (!columnExists(df, columnName)) {
            return false;
        }
        StructField field = df.schema().apply(columnName);
        return field.dataType().equals(DataTypes.IntegerType)
            || field.dataType().equals(DataTypes.LongType)
            || field.dataType().equals(DataTypes.DoubleType)
            || field.dataType().equals(DataTypes.FloatType)
            || field.dataType().equals(DataTypes.ShortType)
            || field.dataType().equals(DataTypes.ByteType);
    }
}
