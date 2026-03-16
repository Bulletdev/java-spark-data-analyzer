package com.dataanalyzer.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Centralizes all application configuration loaded from
 * {@code application.properties} on the classpath.
 * Falls back to sensible defaults when the file is absent.
 */
public class SparkConfig {

    /** Loaded properties. */
    private final Properties props;

    /** Creates a new SparkConfig, loading from classpath. */
    public SparkConfig() {
        props = new Properties();
        try (InputStream in = getClass()
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            // Use defaults if file is not found
        }
    }

    /** @return Spark application name */
    public String getAppName() {
        return props.getProperty("spark.app.name", "Java Data Analyzer");
    }

    /** @return Spark master URL */
    public String getMaster() {
        return props.getProperty("spark.master", "local[*]");
    }

    /** @return whether the Spark Web UI should be enabled */
    public boolean isUiEnabled() {
        return Boolean.parseBoolean(
            props.getProperty("spark.ui.enabled", "false"));
    }

    /** @return Spark log level (e.g. ERROR, WARN) */
    public String getLogLevel() {
        return props.getProperty("spark.log.level", "ERROR");
    }

    /** @return path to the Spark SQL warehouse directory */
    public String getWarehouseDir() {
        return props.getProperty(
            "spark.warehouse.dir", "spark-warehouse");
    }

    /** @return path to the bundled sample CSV file */
    public String getSampleDataPath() {
        return props.getProperty(
            "app.sample.data.path",
            "src/main/resources/dados_vendas.csv");
    }

    /** @return default output directory for saved results */
    public String getOutputDir() {
        return props.getProperty(
            "app.output.dir",
            "src/main/resources/results");
    }
}
