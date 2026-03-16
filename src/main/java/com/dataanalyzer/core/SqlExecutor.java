package com.dataanalyzer.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes arbitrary Spark SQL queries against a registered DataFrame
 * view.
 *
 * <p>The DataFrame is registered as a temporary view named {@code "df"}
 * before each query execution, making it addressable from SQL as
 * {@code FROM df}.
 */
public class SqlExecutor {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(SqlExecutor.class);

    /** Name of the temporary view. */
    private static final String VIEW_NAME = "df";

    /** Active Spark session used to execute queries. */
    private final SparkSession spark;

    /**
     * @param sparkSession active SparkSession used to execute queries
     */
    public SqlExecutor(final SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    /**
     * Registers the DataFrame as a temporary view and executes the SQL.
     *
     * <p>The view name is always {@code "df"} — users should reference
     * it as {@code SELECT ... FROM df}.
     *
     * @param df    DataFrame to register as view {@code df}
     * @param query SQL query string (must not be blank)
     * @return result DataFrame produced by the query
     * @throws IllegalArgumentException if the query is {@code null}
     *                                  or blank
     * @throws RuntimeException         wrapping any Spark error
     *                                  encountered at parse/analysis
     *                                  time
     */
    public Dataset<Row> execute(
            final Dataset<Row> df,
            final String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException(
                "Query SQL não pode ser vazia.");
        }

        LOG.info("Registering temp view '{}' and executing SQL",
            VIEW_NAME);
        df.createOrReplaceTempView(VIEW_NAME);

        try {
            Dataset<Row> result = spark.sql(query);
            LOG.info("SQL executed successfully");
            return result;
        } catch (Exception e) {
            throw new RuntimeException(
                "Erro SQL: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the name of the temporary view exposed to the user.
     *
     * @return the view name, always {@code "df"}
     */
    public String getViewName() {
        return VIEW_NAME;
    }
}
