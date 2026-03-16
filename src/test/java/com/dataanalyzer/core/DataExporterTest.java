package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DataExporterTest extends SparkTestBase {

    private DataExporter exporter;
    private Dataset<Row> df;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        exporter = new DataExporter();
        String csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        df = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(csvPath);
    }

    @Test
    void export_csv_createsOutputDirectory() {
        String path = tempDir.resolve("out_csv").toString();
        exporter.export(df, path, ExportFormat.CSV, false);

        assertTrue(new File(path).exists());
        assertTrue(new File(path).isDirectory());
    }

    @Test
    void export_csv_containsPartFiles() {
        String path = tempDir.resolve("out_csv_parts").toString();
        exporter.export(df, path, ExportFormat.CSV, false);

        File[] parts = new File(path).listFiles(
            f -> f.getName().startsWith("part-"));
        assertNotNull(parts);
        assertTrue(parts.length > 0);
    }

    @Test
    void export_parquet_createsOutputDirectory() {
        String path = tempDir.resolve("out_parquet").toString();
        exporter.export(df, path, ExportFormat.PARQUET, false);

        assertTrue(new File(path).exists());
    }

    @Test
    void export_json_createsOutputDirectory() {
        String path = tempDir.resolve("out_json").toString();
        exporter.export(df, path, ExportFormat.JSON, false);

        assertTrue(new File(path).exists());
    }

    @Test
    void export_overwrite_replacesExistingDirectory() throws IOException {
        String path = tempDir.resolve("out_overwrite").toString();
        exporter.export(df, path, ExportFormat.CSV, false, true, ",");
        // Second export with overwrite=true should not throw
        assertDoesNotThrow(() ->
            exporter.export(df, path, ExportFormat.CSV, true, true, ",")
        );
    }

    @Test
    void export_emptyPath_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            exporter.export(df, "", ExportFormat.CSV, false)
        );
    }

    @Test
    void export_csvIsReadableBySparkAfterWrite() {
        String path = tempDir.resolve("out_readable").toString();
        exporter.export(df, path, ExportFormat.CSV, false, true, ",");

        Dataset<Row> reloaded = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(path);

        assertEquals(df.count(), reloaded.count());
    }

    @Test
    void export_singleFile_createsSinglePartFile() {
        String path = tempDir.resolve("out_single").toString();
        exporter.export(df, path, ExportFormat.CSV, false, true, true, ",");

        java.io.File[] parts = new java.io.File(path).listFiles(
            f -> f.getName().startsWith("part-"));
        assertNotNull(parts);
        assertEquals(1, parts.length);
    }
}
