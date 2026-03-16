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

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataLoader.class);

    /** Active Spark session. */
    private final SparkSession spark;

    /** Application configuration for sample data path. */
    private final SparkConfig config;

    /**
     * @param sparkSession active SparkSession
     * @param sparkConfig  application configuration
     *                     (used for sample data path)
     */
    public DataLoader(
            final SparkSession sparkSession,
            final SparkConfig sparkConfig) {
        this.spark = sparkSession;
        this.config = sparkConfig;
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
    public Dataset<Row> load(
            final String path,
            final boolean hasHeader,
            final String delimiter) {
        if (!new File(path).exists()) {
            throw new IllegalArgumentException(
                "Arquivo não encontrado: " + path);
        }

        LOG.info("Loading file: {}", path);
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
        LOG.info("Loaded {} rows, {} columns from {}",
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

    /**
     * Loads a Parquet file or directory into a cached DataFrame.
     *
     * @param path path to the Parquet file or directory
     * @return loaded and cached DataFrame
     * @throws IllegalArgumentException if the path does not exist
     */
    public Dataset<Row> loadParquet(final String path) {
        if (!new File(path).exists()) {
            throw new IllegalArgumentException(
                "Arquivo/diretório Parquet não encontrado: " + path);
        }

        LOG.info("Loading Parquet: {}", path);
        System.out.println("Carregando dados Parquet...");

        Dataset<Row> df = spark.read().parquet(path);

        df.cache();
        long rowCount = df.count();

        System.out.printf(
            "Dados carregados! Linhas: %d | Colunas: %d%n",
            rowCount, df.columns().length);
        LOG.info("Loaded {} rows, {} columns from Parquet {}",
            rowCount, df.columns().length, path);

        return df;
    }

    /**
     * Loads a JSON file (one JSON object per line) into a cached
     * DataFrame.
     *
     * @param path path to the JSON file or directory
     * @return loaded and cached DataFrame
     * @throws IllegalArgumentException if the path does not exist
     */
    public Dataset<Row> loadJson(final String path) {
        if (!new File(path).exists()) {
            throw new IllegalArgumentException(
                "Arquivo/diretório JSON não encontrado: " + path);
        }

        LOG.info("Loading JSON: {}", path);
        System.out.println("Carregando dados JSON...");

        Dataset<Row> df = spark.read()
            .option("inferSchema", "true")
            .json(path);

        df.cache();
        long rowCount = df.count();

        System.out.printf(
            "Dados carregados! Linhas: %d | Colunas: %d%n",
            rowCount, df.columns().length);
        LOG.info("Loaded {} rows, {} columns from JSON {}",
            rowCount, df.columns().length, path);

        return df;
    }
}
