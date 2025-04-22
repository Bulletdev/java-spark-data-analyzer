package com.dataanalyzer;

import org.apache.spark.sql.SparkSession;
import com.dataanalyzer.SparkOperations;
import com.dataanalyzer.UserInterface;

import java.io.File;

public class DataAnalyzer {

    private SparkSession spark;
    private SparkOperations sparkOperations;
    private UserInterface userInterface;

    public static void main(String[] args) {
        DataAnalyzer analyzer = new DataAnalyzer();
        analyzer.initialize();
        analyzer.run();
        analyzer.shutdown();
    }

    public void initialize() {
        System.setProperty("java.security.auth.login.config", "");
        System.setProperty("hadoop.home.dir", new File("").getAbsolutePath());

        spark = SparkSession.builder()
                .appName("Java Data Analyzer")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "spark-warehouse")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        sparkOperations = new SparkOperations(spark);
        userInterface = new UserInterface(sparkOperations);

        System.out.println("Spark inicializado com sucesso!");
        System.out.println("Versão: " + spark.version());
    }

    public void run() {
        userInterface.run();
    }

    private void shutdown() {
        if (spark != null) {
            spark.stop();
            System.out.println("Sessão Spark encerrada. Até mais!");
        }
    }
}