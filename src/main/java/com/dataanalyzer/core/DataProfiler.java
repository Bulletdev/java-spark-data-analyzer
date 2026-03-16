package com.dataanalyzer.core;

import com.dataanalyzer.util.SchemaValidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;

/**
 * Generates a comprehensive data profile for a Spark DataFrame.
 *
 * <p>For each column the profile reports: data type, total null count,
 * null percentage, distinct-value count, and (for numeric columns) the
 * min and max values obtained from {@code df.describe()}.
 */
public class DataProfiler {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataProfiler.class);

    /** Width of the separator line for the profile table. */
    private static final int COLUMN_WIDTH = 110;

    /** Factor for converting ratio to percentage. */
    private static final double PERCENT_FACTOR = 100.0;

    /**
     * Prints a formatted profile table to stdout.
     *
     * <p>Each row corresponds to one column in the DataFrame and shows:
     * name, type, null count, null%, distinct count, min (numeric),
     * max (numeric only).
     *
     * @param df input DataFrame to profile (must not be {@code null})
     */
    public void profile(final Dataset<Row> df) {
        long totalRows = df.count();

        System.out.println("\n--- Perfil de Dados ---");
        System.out.printf(
            "%-25s %-15s %10s %10s %15s %15s %15s%n",
            "Coluna", "Tipo", "Nulos", "Nulos%",
            "Distintos", "Min", "Max");
        System.out.println("-".repeat(COLUMN_WIDTH));

        Dataset<Row> stats = df.describe();

        for (String colName : df.columns()) {
            String typeName =
                df.schema().apply(colName).dataType().simpleString();
            long nullCount =
                df.filter(col(colName).isNull()).count();
            double nullPct = totalRows > 0
                ? (nullCount * PERCENT_FACTOR) / totalRows
                : 0.0;
            long distinctCount =
                df.select(colName).distinct().count();

            String minVal = "-";
            String maxVal = "-";
            if (SchemaValidator.isNumeric(df, colName)) {
                minVal = getStat(stats, "min", colName);
                maxVal = getStat(stats, "max", colName);
            }

            System.out.printf(
                "%-25s %-15s %10d %9.1f%% %15d %15s %15s%n",
                colName, typeName, nullCount, nullPct,
                distinctCount, minVal, maxVal);
        }

        System.out.println("-".repeat(COLUMN_WIDTH));
        System.out.println("Total de linhas: " + totalRows);
        LOG.info(
            "Profile printed for DataFrame with {} columns and"
            + " {} rows",
            df.columns().length, totalRows);
    }

    private String getStat(
            final Dataset<Row> stats,
            final String statName,
            final String colName) {
        try {
            Row row = stats.filter(
                col("summary").equalTo(statName)).first();
            Object val = row.getAs(colName);
            return val == null ? "-" : val.toString();
        } catch (Exception e) {
            return "-";
        }
    }
}
