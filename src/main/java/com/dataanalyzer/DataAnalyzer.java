package com.dataanalyzer;

import com.dataanalyzer.config.SparkConfig;
import com.dataanalyzer.core.DataAggregator;
import com.dataanalyzer.core.DataExporter;
import com.dataanalyzer.core.DataLoader;
import com.dataanalyzer.core.DataTransformer;
import com.dataanalyzer.session.SparkSessionManager;
import com.dataanalyzer.ui.InputReader;
import com.dataanalyzer.ui.MenuRenderer;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Application entry point.
 *
 * <p>Wires together configuration, the Spark session, all service classes,
 * and the user interface before handing control to the interactive loop.
 */
public class DataAnalyzer {

    private static final Logger log =
        LoggerFactory.getLogger(DataAnalyzer.class);

    private SparkSessionManager sessionManager;
    private UserInterface userInterface;

    public static void main(String[] args) {
        DataAnalyzer analyzer = new DataAnalyzer();
        analyzer.initialize();
        analyzer.run();
        analyzer.shutdown();
    }

    /** Initialises all components and starts the SparkSession. */
    public void initialize() {
        SparkConfig config = new SparkConfig();
        sessionManager = new SparkSessionManager(config);
        SparkSession spark = sessionManager.initialize();

        DataLoader loader       = new DataLoader(spark, config);
        DataTransformer transformer = new DataTransformer();
        DataAggregator aggregator   = new DataAggregator();
        DataExporter exporter       = new DataExporter();
        InputReader inputReader     = new InputReader(new Scanner(System.in));
        MenuRenderer menuRenderer   = new MenuRenderer();

        userInterface = new UserInterface(
            loader, transformer, aggregator,
            exporter, inputReader, menuRenderer);

        System.out.println("Java Data Analyzer iniciado!");
        System.out.println("Apache Spark " + spark.version());
        log.info("Application initialized.");
    }

    /** Starts the interactive CLI loop. */
    public void run() {
        userInterface.run();
    }

    private void shutdown() {
        sessionManager.shutdown();
        System.out.println("Sessão Spark encerrada. Até mais!");
        log.info("Application shutdown.");
    }
}
