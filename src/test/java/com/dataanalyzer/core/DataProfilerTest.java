package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Unit tests for {@link DataProfiler}.
 */
class DataProfilerTest extends SparkTestBase {

    private DataProfiler profiler;
    private Dataset<Row> df;

    @BeforeEach
    void setUp() {
        profiler = new DataProfiler();
        String csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        df = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(csvPath);
    }

    @Test
    void profile_doesNotThrow() {
        assertDoesNotThrow(() -> profiler.profile(df));
    }

    @Test
    void profile_worksOnEmptyDataFrame() {
        StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id",   DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType,  true)
        });
        Dataset<Row> emptyDf = spark.createDataFrame(
            Collections.emptyList(), schema);

        assertDoesNotThrow(() -> profiler.profile(emptyDf));
    }
}
