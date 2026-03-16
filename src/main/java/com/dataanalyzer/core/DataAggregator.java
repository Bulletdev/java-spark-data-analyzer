package com.dataanalyzer.core;

import com.dataanalyzer.util.SchemaValidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Performs aggregation operations on Spark DataFrames.
 *
 * <p>All methods are stateless — they receive and return DataFrames
 * without holding any internal state.
 */
public class DataAggregator {

    private static final Logger log =
        LoggerFactory.getLogger(DataAggregator.class);

    /**
     * Aggregates a column using the specified function,
     * optionally grouped by another column.
     *
     * @param df              input DataFrame
     * @param aggregateColumn column whose values are aggregated
     * @param function        aggregation function to apply
     * @param groupByColumn   column to group by, or {@code null}/empty for
     *                        a global aggregation
     * @return aggregated DataFrame
     * @throws IllegalArgumentException if any referenced column does not exist
     */
    public Dataset<Row> aggregate(
            Dataset<Row> df,
            String aggregateColumn,
            AggFunction function,
            String groupByColumn) {

        if (!SchemaValidator.columnExists(df, aggregateColumn)) {
            throw new IllegalArgumentException(
                "Coluna para agregação não encontrada: " + aggregateColumn);
        }

        boolean hasGroupBy = groupByColumn != null && !groupByColumn.isEmpty();
        if (hasGroupBy && !SchemaValidator.columnExists(df, groupByColumn)) {
            throw new IllegalArgumentException(
                "Coluna para agrupamento não encontrada: " + groupByColumn);
        }

        RelationalGroupedDataset grouped = hasGroupBy
            ? df.groupBy(col(groupByColumn))
            : null;

        String resultCol = function.name().toLowerCase() + "_" + aggregateColumn;

        Dataset<Row> result = applyFunction(
            df, grouped, function, aggregateColumn, resultCol, groupByColumn);

        result.cache();
        result.count();

        log.info("Aggregation: {} on {} groupBy={}",
            function, aggregateColumn, groupByColumn);
        return result;
    }

    private Dataset<Row> applyFunction(
            Dataset<Row> df,
            RelationalGroupedDataset grouped,
            AggFunction function,
            String column,
            String alias,
            String groupByColumn) {

        switch (function) {
            case AVG:
                return grouped != null
                    ? grouped.agg(avg(column).alias(alias))
                             .orderBy(col(groupByColumn))
                    : df.agg(avg(column).alias(alias));
            case SUM:
                return grouped != null
                    ? grouped.agg(sum(column).alias(alias))
                             .orderBy(col(groupByColumn))
                    : df.agg(sum(column).alias(alias));
            case MIN:
                return grouped != null
                    ? grouped.agg(min(column).alias(alias))
                             .orderBy(col(groupByColumn))
                    : df.agg(min(column).alias(alias));
            case MAX:
                return grouped != null
                    ? grouped.agg(max(column).alias(alias))
                             .orderBy(col(groupByColumn))
                    : df.agg(max(column).alias(alias));
            case COUNT:
                return grouped != null
                    ? grouped.agg(count(column).alias(alias))
                             .orderBy(col(groupByColumn))
                    : df.agg(count(column).alias(alias));
            default:
                throw new IllegalArgumentException(
                    "Função não suportada: " + function);
        }
    }
}
