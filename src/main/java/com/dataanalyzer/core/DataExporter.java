package com.dataanalyzer.core;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exports a Spark DataFrame to the filesystem in various formats.
 *
 * <p>Spark writes partitioned output to a directory; the resulting files
 * can be merged externally if a single-file output is required.
 */
public class DataExporter {

    private static final Logger log =
        LoggerFactory.getLogger(DataExporter.class);

    /**
     * Exports the DataFrame using default CSV options
     * (header enabled, comma delimiter).
     *
     * @param df        DataFrame to export
     * @param path      destination directory path
     * @param format    output format
     * @param overwrite {@code true} to overwrite an existing directory
     */
    public void export(
            Dataset<Row> df, String path, ExportFormat format,
            boolean overwrite) {
        export(df, path, format, overwrite, true, ",");
    }

    /**
     * Exports the DataFrame with full control over CSV formatting options.
     *
     * @param df           DataFrame to export
     * @param path         destination directory path
     * @param format       output format
     * @param overwrite    {@code true} to overwrite an existing directory
     * @param csvHeader    include a header row (CSV only)
     * @param csvDelimiter field separator character (CSV only)
     * @throws IllegalArgumentException if the path is blank or the format
     *                                  is not supported
     */
    public void export(
            Dataset<Row> df,
            String path,
            ExportFormat format,
            boolean overwrite,
            boolean csvHeader,
            String csvDelimiter) {

        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException(
                "Caminho de destino não pode ser vazio.");
        }

        String saveMode = overwrite ? "overwrite" : "errorifexists";
        log.info("Exporting to {} as {} (mode={})", path, format, saveMode);
        System.out.println("Salvando dados em: " + path);

        DataFrameWriter<Row> writer = df.write().mode(saveMode);

        switch (format) {
            case CSV:
                writer.option("header", csvHeader)
                      .option("delimiter", csvDelimiter)
                      .csv(path);
                break;
            case PARQUET:
                writer.parquet(path);
                break;
            case JSON:
                writer.json(path);
                break;
            default:
                throw new IllegalArgumentException(
                    "Formato não suportado: " + format);
        }

        System.out.println("Dados salvos com sucesso em: " + path);
        log.info("Export completed: {}", path);
    }
}
