package com.dataanalyzer;

import com.dataanalyzer.config.SparkConfig;
import com.dataanalyzer.core.DataAggregator;
import com.dataanalyzer.core.DataExporter;
import com.dataanalyzer.core.DataJoiner;
import com.dataanalyzer.core.DataLoader;
import com.dataanalyzer.core.DataProfiler;
import com.dataanalyzer.core.DataTransformer;
import com.dataanalyzer.core.SqlExecutor;
import com.dataanalyzer.session.SparkSessionManager;
import com.dataanalyzer.ui.InputReader;
import com.dataanalyzer.ui.MenuRenderer;
import com.dataanalyzer.util.OperationHistory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * Application entry point.
 *
 * <p>Wires together configuration, the Spark session, all service classes,
 * and the user interface before handing control to the interactive loop.
 */
public class DataAnalyzer {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataAnalyzer.class);

    /** Session manager for the Spark lifecycle. */
    private SparkSessionManager sessionManager;

    /** The interactive user interface. */
    private UserInterface userInterface;

    /**
     * Main entry point.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) {
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

        DataLoader loader           = new DataLoader(spark, config);
        DataTransformer transformer = new DataTransformer();
        DataAggregator aggregator   = new DataAggregator();
        DataExporter exporter       = new DataExporter();
        SqlExecutor sqlExecutor     = new SqlExecutor(spark);
        DataJoiner joiner           = new DataJoiner();
        DataProfiler profiler       = new DataProfiler();
        OperationHistory history    = new OperationHistory();
        drainStdin();
        InputReader inputReader
            = new InputReader(new Scanner(System.in));
        MenuRenderer menuRenderer   = new MenuRenderer();

        userInterface = new UserInterface(
            new UserInterfaceServices.Builder()
                .loader(loader)
                .transformer(transformer)
                .aggregator(aggregator)
                .exporter(exporter)
                .input(inputReader)
                .menu(menuRenderer)
                .sqlExecutor(sqlExecutor)
                .joiner(joiner)
                .profiler(profiler)
                .history(history)
                .build());

        System.out.println("Java Data Analyzer iniciado!");
        System.out.println("Apache Spark " + spark.version());
        LOG.info("Application initialized.");
    }

    /** Starts the interactive CLI loop. */
    public void run() {
        userInterface.run();
    }

    /**
     * Drains any bytes injected into stdin during Spark initialisation.
     *
     * <p>Spark/Hadoop internals sometimes push a newline into stdin while
     * starting up, which would cause the first {@code Scanner.nextLine()}
     * call to return an empty string immediately.
     */
    private void drainStdin() {
        try {
            int available = System.in.available();
            if (available > 0) {
                System.in.skip(available);
            }
        } catch (IOException e) {
            LOG.warn("Could not drain stdin: {}", e.getMessage());
        }
    }

    private void shutdown() {
        sessionManager.shutdown();
        System.out.println("Sessão Spark encerrada. Até mais!");
        LOG.info("Application shutdown.");
    }
}
