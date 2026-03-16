package com.dataanalyzer.core;

import com.dataanalyzer.util.SchemaValidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

/**
 * Performs join operations between two Spark DataFrames.
 *
 * <p>All methods are stateless — they accept DataFrames as arguments
 * and return a new DataFrame without retaining any internal state.
 */
public class DataJoiner {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataJoiner.class);

    /**
     * Joins two DataFrames on a single common key column.
     *
     * @param left      left DataFrame
     * @param right     right DataFrame
     * @param keyColumn column name that must be present in both
     * @param joinType  type of join to perform
     * @return joined DataFrame
     * @throws IllegalArgumentException if {@code keyColumn} does not
     *                                  exist in either DataFrame
     */
    public Dataset<Row> join(
            final Dataset<Row> left,
            final Dataset<Row> right,
            final String keyColumn,
            final JoinType joinType) {

        validateColumnInBoth(left, right, keyColumn);

        LOG.info("Joining on '{}' ({} join)",
            keyColumn, joinType.getSparkName());
        Dataset<Row> result = left.join(right,
            left.col(keyColumn).equalTo(
                right.col(keyColumn)),
            joinType.getSparkName());

        LOG.info("Join completed — {} rows", result.count());
        return result;
    }

    /**
     * Joins two DataFrames on multiple common key columns.
     *
     * @param left       left DataFrame
     * @param right      right DataFrame
     * @param keyColumns list of column names in both DataFrames
     * @param joinType   type of join to perform
     * @return joined DataFrame
     * @throws IllegalArgumentException if any column in
     *                                  {@code keyColumns} does not
     *                                  exist in either DataFrame
     */
    public Dataset<Row> join(
            final Dataset<Row> left,
            final Dataset<Row> right,
            final List<String> keyColumns,
            final JoinType joinType) {

        if (keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                "A lista de colunas chave não pode ser vazia.");
        }

        for (String key : keyColumns) {
            validateColumnInBoth(left, right, key);
        }

        LOG.info("Joining on {} ({} join)",
            keyColumns, joinType.getSparkName());

        Seq<String> keySeq =
            JavaConverters
                .collectionAsScalaIterableConverter(keyColumns)
                .asScala().toSeq();

        Dataset<Row> result = left.join(
            right, keySeq, joinType.getSparkName());
        LOG.info("Join completed — {} rows", result.count());
        return result;
    }

    private void validateColumnInBoth(
            final Dataset<Row> left,
            final Dataset<Row> right,
            final String column) {

        if (!SchemaValidator.columnExists(left, column)) {
            throw new IllegalArgumentException(
                "Coluna chave não encontrada no DataFrame"
                + " esquerdo: " + column);
        }
        if (!SchemaValidator.columnExists(right, column)) {
            throw new IllegalArgumentException(
                "Coluna chave não encontrada no DataFrame"
                + " direito: " + column);
        }
    }
}
