package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SqlExecutor}.
 */
class SqlExecutorTest extends SparkTestBase {

    private SqlExecutor executor;
    private Dataset<Row> df;

    @BeforeEach
    void setUp() {
        executor = new SqlExecutor(spark);
        String csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        df = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(csvPath);
    }

    @Test
    void execute_selectAll_returnsAllRows() {
        Dataset<Row> result = executor.execute(df, "SELECT * FROM df");

        assertEquals(df.count(), result.count());
    }

    @Test
    void execute_withFilter_returnsFilteredRows() {
        Dataset<Row> result = executor.execute(df,
            "SELECT * FROM df WHERE Categoria = 'Moveis'");

        assertTrue(result.count() > 0);
        assertTrue(result.count() < df.count());
    }

    @Test
    void execute_withGroupBy_returnsAggregation() {
        Dataset<Row> result = executor.execute(df,
            "SELECT Categoria, COUNT(*) as qtd FROM df GROUP BY Categoria");

        assertNotNull(result);
        assertTrue(result.count() > 0);
        assertTrue(
            java.util.Arrays.asList(result.columns()).contains("qtd"));
    }

    @Test
    void execute_invalidSql_throwsException() {
        assertThrows(RuntimeException.class, () ->
            executor.execute(df, "SELECT * FROM nonexistent_table_xyz")
        );
    }

    @Test
    void execute_emptyQuery_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            executor.execute(df, "")
        );
    }

    @Test
    void execute_blankQuery_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            executor.execute(df, "   ")
        );
    }

    @Test
    void getViewName_returnsCorrectName() {
        assertEquals("df", executor.getViewName());
    }
}
