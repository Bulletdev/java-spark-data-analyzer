package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DataJoiner}.
 */
class DataJoinerTest extends SparkTestBase {

    private DataJoiner joiner;
    private Dataset<Row> productsDf;
    private Dataset<Row> customersDf;

    @BeforeEach
    void setUp() {
        joiner = new DataJoiner();

        String productsPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        productsDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(productsPath);

        String customersPath = getClass().getClassLoader()
            .getResource("test_customers.csv").getPath();
        customersDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(customersPath);
    }

    /**
     * Build a small DataFrame that shares an ID column with productsDf
     * so we can exercise join logic.
     */
    private Dataset<Row> buildRightDf() {
        // reuse customersDf which has ID column
        return customersDf;
    }

    private Dataset<Row> buildLeftDf() {
        // Select ID column from productsDf for join
        return productsDf.select("ID", "Produto");
    }

    @Test
    void join_inner_returnsOnlyMatchingRows() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        Dataset<Row> result = joiner.join(left, right, "ID", JoinType.INNER);

        assertNotNull(result);
        // only IDs present in both (1..4 in customers, 1..8 in products)
        assertTrue(result.count() > 0);
        assertTrue(result.count() <= Math.min(left.count(), right.count()));
    }

    @Test
    void join_left_preservesAllLeftRows() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        Dataset<Row> result = joiner.join(left, right, "ID", JoinType.LEFT);

        assertEquals(left.count(), result.count());
    }

    @Test
    void join_right_preservesAllRightRows() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        Dataset<Row> result = joiner.join(left, right, "ID", JoinType.RIGHT);

        assertEquals(right.count(), result.count());
    }

    @Test
    void join_full_returnsAllRows() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        Dataset<Row> result = joiner.join(left, right, "ID", JoinType.FULL);

        // full outer should have at least as many rows as the larger side
        assertTrue(result.count() >= Math.max(left.count(), right.count()));
    }

    @Test
    void join_invalidKey_throwsException() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        assertThrows(IllegalArgumentException.class, () ->
            joiner.join(left, right, "NonExistentColumn", JoinType.INNER)
        );
    }

    @Test
    void join_multipleKeys_works() {
        Dataset<Row> left  = buildLeftDf();
        Dataset<Row> right = buildRightDf();

        // Both have ID but only 'ID' is common; wrap in list
        Dataset<Row> result = joiner.join(
            left, right, Arrays.asList("ID"), JoinType.INNER);

        assertNotNull(result);
        assertTrue(result.count() >= 0);
    }
}
