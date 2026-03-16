package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataAggregatorTest extends SparkTestBase {

    private DataAggregator aggregator;
    private Dataset<Row> df;

    @BeforeEach
    void setUp() {
        aggregator = new DataAggregator();
        String csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        df = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(csvPath);
    }

    @Test
    void aggregate_count_returnsOneRow_withoutGroupBy() {
        Dataset<Row> result = aggregator.aggregate(
            df, "ID", AggFunction.COUNT, null);

        assertEquals(1, result.count());
    }

    @Test
    void aggregate_sum_producesPositiveValue() {
        Dataset<Row> result = aggregator.aggregate(
            df, "Preco", AggFunction.SUM, null);

        Row row = result.first();
        double sum = row.getAs("sum_Preco");
        assertTrue(sum > 0);
    }

    @Test
    void aggregate_avg_producesPositiveValue() {
        Dataset<Row> result = aggregator.aggregate(
            df, "Preco", AggFunction.AVG, null);

        Row row = result.first();
        double avg = row.getAs("avg_Preco");
        assertTrue(avg > 0);
    }

    @Test
    void aggregate_min_lessThanMax() {
        Dataset<Row> min = aggregator.aggregate(
            df, "Preco", AggFunction.MIN, null);
        Dataset<Row> max = aggregator.aggregate(
            df, "Preco", AggFunction.MAX, null);

        double minVal = min.first().getAs("min_Preco");
        double maxVal = max.first().getAs("max_Preco");

        assertTrue(minVal <= maxVal);
    }

    @Test
    void aggregate_withGroupBy_returnsOneRowPerGroup() {
        Dataset<Row> result = aggregator.aggregate(
            df, "Preco", AggFunction.COUNT, "Categoria");

        // test_data.csv has 2 categories: Eletronicos and Moveis
        assertEquals(2, result.count());
    }

    @Test
    void aggregate_invalidColumn_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            aggregator.aggregate(df, "Ghost", AggFunction.SUM, null)
        );
    }

    @Test
    void aggregate_invalidGroupByColumn_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            aggregator.aggregate(df, "Preco", AggFunction.SUM, "Ghost")
        );
    }

    @Test
    void aggregate_resultColumnNameFollowsConvention() {
        Dataset<Row> result = aggregator.aggregate(
            df, "Preco", AggFunction.AVG, null);

        assertTrue(
            java.util.Arrays.asList(result.columns()).contains("avg_Preco"),
            "Expected column 'avg_Preco' in result schema"
        );
    }
}
