package com.dataanalyzer.core;

import com.dataanalyzer.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Loads tabular data from the filesystem into a Spark DataFrame.
 *
 * <p>Supports CSV with configurable header and delimiter settings.
 * The resulting DataFrame is cached automatically.
 */
public class DataLoader {

    private static final Logger log =
        LoggerFactory.getLogger(DataLoader.class);

    private final SparkSession spark;
    private final SparkConfig config;

    /**
     * @param spark  active SparkSession
     * @param config application configuration (used for sample data path)
     */
    public DataLoader(SparkSession spark, SparkConfig config) {
        this.spark = spark;
        this.config = config;
    }

    /**
     * Loads a CSV file into a cached DataFrame.
     *
     * @param path      path to the CSV file
     * @param hasHeader whether the first row is a header
     * @param delimiter field separator character
     * @return loaded and cached DataFrame
     * @throws IllegalArgumentException if the file does not exist
     */
    public Dataset<Row> load(String path, boolean hasHeader, String delimiter) {
        if (!new File(path).exists()) {
            throw new IllegalArgumentException("Arquivo não encontrado: " + path);
        }

        log.info("Loading file: {}", path);
        System.out.println("Carregando dados...");

        Dataset<Row> df = spark.read()
            .option("header", hasHeader)
            .option("delimiter", delimiter)
            .option("inferSchema", "true")
            .csv(path);

        df.cache();
        long rowCount = df.count();

        System.out.printf(
            "Dados carregados! Linhas: %d | Colunas: %d%n",
            rowCount, df.columns().length);
        log.info("Loaded {} rows, {} columns from {}",
            rowCount, df.columns().length, path);

        return df;
    }

    /**
     * Loads the bundled sample CSV file.
     *
     * @return sample DataFrame
     */
    public Dataset<Row> loadSample() {
        return load(config.getSampleDataPath(), true, ",");
    }
}
