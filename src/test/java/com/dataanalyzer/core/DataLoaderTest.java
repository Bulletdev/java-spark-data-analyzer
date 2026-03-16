package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import com.dataanalyzer.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DataLoaderTest extends SparkTestBase {

    private DataLoader loader;
    private String csvPath;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        SparkConfig config = new SparkConfig();
        loader = new DataLoader(spark, config);
        csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
    }

    @Test
    void load_returnsNonEmptyDataFrame_forValidCsv() {
        Dataset<Row> df = loader.load(csvPath, true, ",");

        assertNotNull(df);
        assertTrue(df.count() > 0);
    }

    @Test
    void load_inferesCorrectColumnCount() {
        Dataset<Row> df = loader.load(csvPath, true, ",");

        assertEquals(5, df.columns().length);
    }

    @Test
    void load_containsExpectedColumns() {
        Dataset<Row> df = loader.load(csvPath, true, ",");

        assertArrayEquals(
            new String[]{"ID", "Produto", "Categoria", "Preco", "Quantidade"},
            df.columns()
        );
    }

    @Test
    void load_throwsException_forNonexistentFile() {
        assertThrows(IllegalArgumentException.class, () ->
            loader.load("/tmp/does_not_exist_abc123.csv", true, ",")
        );
    }

    @Test
    void load_loadsAllRows() {
        Dataset<Row> df = loader.load(csvPath, true, ",");

        assertEquals(8, df.count());
    }

    @Test
    void load_withoutHeader_addsDefaultColumnNames() {
        Dataset<Row> df = loader.load(csvPath, false, ",");

        // When header=false, Spark names columns _c0, _c1, ...
        assertTrue(df.columns()[0].startsWith("_c"));
    }

    @Test
    void loadParquet_createsDataFrame() {
        // Write a Parquet version of the test data first
        Dataset<Row> original = loader.load(csvPath, true, ",");
        String parquetPath = tempDir.resolve("test_data.parquet").toString();
        original.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> result = loader.loadParquet(parquetPath);

        assertNotNull(result);
        assertEquals(original.count(), result.count());
    }

    @Test
    void loadParquet_throwsException_forNonexistentPath() {
        assertThrows(IllegalArgumentException.class, () ->
            loader.loadParquet("/tmp/nonexistent_parquet_abc123")
        );
    }

    @Test
    void loadJson_createsDataFrame() {
        // Write a JSON version of the test data first
        Dataset<Row> original = loader.load(csvPath, true, ",");
        String jsonPath = tempDir.resolve("test_data.json").toString();
        original.write().mode("overwrite").json(jsonPath);

        Dataset<Row> result = loader.loadJson(jsonPath);

        assertNotNull(result);
        assertEquals(original.count(), result.count());
    }

    @Test
    void loadJson_throwsException_forNonexistentPath() {
        assertThrows(IllegalArgumentException.class, () ->
            loader.loadJson("/tmp/nonexistent_json_abc123")
        );
    }
}
