package com.dataanalyzer;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for all Spark integration tests.
 *
 * <p>Provides a single shared {@link SparkSession} for the entire test suite,
 * avoiding the overhead of creating and destroying a session per test class.
 */
public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
            .master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "target/test-warehouse")
            .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null && !spark.sparkContext().isStopped()) {
            spark.stop();
        }
    }
}
