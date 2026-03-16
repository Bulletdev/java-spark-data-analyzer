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

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataExporter.class);

    /**
     * Exports the DataFrame using default CSV options
     * (header enabled, comma delimiter, multi-part output).
     *
     * @param df        DataFrame to export
     * @param path      destination directory path
     * @param format    output format
     * @param overwrite {@code true} to overwrite an existing directory
     */
    public void export(
            final Dataset<Row> df,
            final String path,
            final ExportFormat format,
            final boolean overwrite) {
        export(df, path, format, overwrite, false, true, ",");
    }

    /**
     * Exports the DataFrame with full control over CSV formatting.
     *
     * <p>This overload uses multi-part output (no coalesce).
     *
     * @param df           DataFrame to export
     * @param path         destination directory path
     * @param format       output format
     * @param overwrite    {@code true} to overwrite an existing dir
     * @param csvHeader    include a header row (CSV only)
     * @param csvDelimiter field separator character (CSV only)
     * @throws IllegalArgumentException if the path is blank or the
     *                                  format is not supported
     */
    public void export(
            final Dataset<Row> df,
            final String path,
            final ExportFormat format,
            final boolean overwrite,
            final boolean csvHeader,
            final String csvDelimiter) {
        export(df, path, format, overwrite, false,
            csvHeader, csvDelimiter);
    }

    /**
     * Exports the DataFrame with full control over all export options
     * including single-file coalescing.
     *
     * @param df           DataFrame to export
     * @param path         destination directory path
     * @param format       output format
     * @param overwrite    {@code true} to overwrite an existing dir
     * @param singleFile   {@code true} to coalesce into one part file;
     *                     note that coalescing can be slow for large
     *                     datasets
     * @param csvHeader    include a header row (CSV only)
     * @param csvDelimiter field separator character (CSV only)
     * @throws IllegalArgumentException if the path is blank or the
     *                                  format is not supported
     */
    public void export(
            final Dataset<Row> df,
            final String path,
            final ExportFormat format,
            final boolean overwrite,
            final boolean singleFile,
            final boolean csvHeader,
            final String csvDelimiter) {

        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException(
                "Caminho de destino não pode ser vazio.");
        }

        String saveMode = overwrite ? "overwrite" : "errorifexists";
        LOG.info("Exporting to {} as {} (mode={}, singleFile={})",
            path, format, saveMode, singleFile);
        System.out.println("Salvando dados em: " + path);

        if (singleFile) {
            System.out.println(
                "Atenção: coalesce pode ser lento para datasets"
                + " grandes.");
        }

        Dataset<Row> dfToWrite = singleFile ? df.coalesce(1) : df;
        DataFrameWriter<Row> writer =
            dfToWrite.write().mode(saveMode);

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
        LOG.info("Export completed: {}", path);
    }
}
