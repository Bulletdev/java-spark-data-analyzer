package com.dataanalyzer.session;

import com.dataanalyzer.config.SparkConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Manages the lifecycle of the {@link SparkSession}.
 * Initialization reads all settings from {@link SparkConfig}.
 */
public class SparkSessionManager {

    private static final Logger log =
        LoggerFactory.getLogger(SparkSessionManager.class);

    private final SparkConfig config;
    private SparkSession session;

    /**
     * @param config application configuration
     */
    public SparkSessionManager(SparkConfig config) {
        this.config = config;
    }

    /**
     * Creates and configures a new {@link SparkSession}.
     *
     * @return the initialized SparkSession
     */
    public SparkSession initialize() {
        System.setProperty(
            "hadoop.home.dir", new File("").getAbsolutePath());

        session = SparkSession.builder()
            .appName(config.getAppName())
            .master(config.getMaster())
            .config("spark.sql.warehouse.dir", config.getWarehouseDir())
            .config("spark.ui.enabled",
                String.valueOf(config.isUiEnabled()))
            .config("spark.driver.host", "localhost")
            .getOrCreate();

        session.sparkContext().setLogLevel(config.getLogLevel());
        log.info("SparkSession initialized. Version: {}", session.version());
        return session;
    }

    /**
     * Returns the active SparkSession.
     *
     * @return the SparkSession, or {@code null} if not yet initialized
     */
    public SparkSession getSession() {
        return session;
    }

    /**
     * Stops the SparkSession if it is still running.
     */
    public void shutdown() {
        if (session != null && !session.sparkContext().isStopped()) {
            session.stop();
            log.info("SparkSession stopped.");
        }
    }
}
