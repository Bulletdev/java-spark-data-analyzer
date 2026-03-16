package com.dataanalyzer.core;

import com.dataanalyzer.util.SchemaValidator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

/**
 * Stateless transformation operations on Spark DataFrames.
 *
 * <p>Every method receives a DataFrame as input and returns a new one.
 * State is never held internally, which makes these methods easy to
 * test in isolation.
 */
public class DataTransformer {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(DataTransformer.class);

    /** Fixed seed used for reproducible sampling. */
    private static final long SAMPLE_SEED = 42L;

    /**
     * Filters the DataFrame using a simple column comparison.
     *
     * @param df       input DataFrame
     * @param column   column name to filter on
     * @param operator comparison operator:
     *                 {@code =, >, <, >=, <=, !=}
     * @param value    value to compare against (numeric or string)
     * @return filtered DataFrame
     * @throws IllegalArgumentException if the column does not exist
     *                                  or the operator is not recognised
     */
    public Dataset<Row> filter(
            final Dataset<Row> df,
            final String column,
            final String operator,
            final String value) {

        if (!SchemaValidator.columnExists(df, column)) {
            throw new IllegalArgumentException(
                "Coluna não encontrada: " + column);
        }

        Column colRef = functions.col(column);
        Object filterValue = parseValue(value);

        Dataset<Row> filtered;
        switch (operator) {
            case "=":
                filtered = df.filter(colRef.equalTo(filterValue));
                break;
            case ">":
                filtered = df.filter(colRef.gt(filterValue));
                break;
            case "<":
                filtered = df.filter(colRef.lt(filterValue));
                break;
            case ">=":
                filtered = df.filter(colRef.geq(filterValue));
                break;
            case "<=":
                filtered = df.filter(colRef.leq(filterValue));
                break;
            case "!=":
                filtered = df.filter(colRef.notEqual(filterValue));
                break;
            default:
                throw new IllegalArgumentException(
                    "Operador inválido: " + operator
                    + ". Válidos: =, >, <, >=, <=, !=");
        }

        long count = filtered.count();
        System.out.println(
            "Filtro aplicado. Linhas resultantes: " + count);
        LOG.info("Filter: {} {} {} → {} rows",
            column, operator, value, count);
        return filtered;
    }

    /**
     * Projects the DataFrame to only the specified columns.
     *
     * @param df      input DataFrame
     * @param columns ordered list of column names to keep
     * @return projected DataFrame
     * @throws IllegalArgumentException if the list is empty or any
     *                                  column does not exist
     */
    public Dataset<Row> selectColumns(
            final Dataset<Row> df,
            final List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(
                "Nenhuma coluna especificada.");
        }
        for (String c : columns) {
            if (!SchemaValidator.columnExists(df, c)) {
                throw new IllegalArgumentException(
                    "Coluna não encontrada: " + c);
            }
        }

        String first = columns.get(0);
        String[] rest =
            columns.subList(1, columns.size()).toArray(new String[0]);
        Dataset<Row> result = rest.length == 0
            ? df.select(first)
            : df.select(first, rest);

        LOG.info("Columns selected: {}", columns);
        return result;
    }

    /**
     * Renames a column in the DataFrame.
     *
     * @param df      input DataFrame
     * @param oldName current name of the column
     * @param newName desired new name
     * @return DataFrame with the column renamed
     * @throws IllegalArgumentException if the old column does not
     *                                  exist, new name is blank, or
     *                                  it already exists
     */
    public Dataset<Row> renameColumn(
            final Dataset<Row> df,
            final String oldName,
            final String newName) {

        if (!SchemaValidator.columnExists(df, oldName)) {
            throw new IllegalArgumentException(
                "Coluna não encontrada: " + oldName);
        }
        if (newName == null || newName.isEmpty()) {
            throw new IllegalArgumentException(
                "Novo nome não pode ser vazio.");
        }
        if (SchemaValidator.columnExists(df, newName)) {
            throw new IllegalArgumentException(
                "Já existe uma coluna com o nome: " + newName);
        }

        Dataset<Row> result = df.withColumnRenamed(oldName, newName);
        System.out.println("Coluna '" + oldName
            + "' renomeada para '" + newName + "'.");
        LOG.info("Column renamed: {} → {}", oldName, newName);
        return result;
    }

    /**
     * Adds a new column derived from a Spark SQL expression.
     *
     * @param df            input DataFrame
     * @param newColumnName name for the new column
     * @param expression    Spark SQL expression
     *                      (e.g. {@code "Preco * Quantidade"})
     * @return DataFrame with the new column appended
     * @throws IllegalArgumentException if the name is blank, already
     *                                  exists, or expression is empty
     */
    public Dataset<Row> addColumn(
            final Dataset<Row> df,
            final String newColumnName,
            final String expression) {

        if (newColumnName == null || newColumnName.isEmpty()) {
            throw new IllegalArgumentException(
                "Nome da nova coluna não pode ser vazio.");
        }
        if (SchemaValidator.columnExists(df, newColumnName)) {
            throw new IllegalArgumentException(
                "Já existe uma coluna com o nome: " + newColumnName);
        }
        if (expression == null || expression.isEmpty()) {
            throw new IllegalArgumentException(
                "Expressão não pode ser vazia.");
        }

        Dataset<Row> result =
            df.withColumn(newColumnName, expr(expression));
        System.out.println("Coluna '" + newColumnName + "' criada.");
        LOG.info("Column added: {} = {}", newColumnName, expression);
        return result;
    }

    /**
     * Sorts the DataFrame by the given columns.
     *
     * @param df          input DataFrame
     * @param columnNames columns to sort by
     * @param ascending   {@code true} for ascending,
     *                    {@code false} for descending
     * @return sorted DataFrame
     * @throws IllegalArgumentException if any column does not exist
     */
    public Dataset<Row> sort(
            final Dataset<Row> df,
            final String[] columnNames,
            final boolean ascending) {

        if (columnNames.length == 0) {
            throw new IllegalArgumentException(
                "Nenhuma coluna especificada para ordenação.");
        }
        for (String c : columnNames) {
            if (!SchemaValidator.columnExists(df, c)) {
                throw new IllegalArgumentException(
                    "Coluna não encontrada: " + c);
            }
        }

        Column[] sortCols = Arrays.stream(columnNames)
            .map(c -> ascending ? col(c) : col(c).desc())
            .toArray(Column[]::new);

        Dataset<Row> result = df.orderBy(sortCols);
        LOG.info("Data sorted by {} (ascending={})",
            Arrays.toString(columnNames), ascending);
        return result;
    }

    /**
     * Removes duplicate rows from the DataFrame.
     *
     * @param df         input DataFrame
     * @param keyColumns columns to use as deduplication key;
     *                   pass an empty array for exact-row dedup
     * @return deduplicated DataFrame
     * @throws IllegalArgumentException if any key column does not exist
     */
    public Dataset<Row> removeDuplicates(
            final Dataset<Row> df,
            final String[] keyColumns) {
        long before = df.count();
        Dataset<Row> result;

        if (keyColumns.length == 0) {
            result = df.distinct();
        } else {
            for (String c : keyColumns) {
                if (!SchemaValidator.columnExists(df, c)) {
                    throw new IllegalArgumentException(
                        "Coluna não encontrada: " + c);
                }
            }
            result = df.dropDuplicates(keyColumns);
        }

        long after = result.count();
        System.out.printf(
            "Duplicatas removidas: %d linhas. Total final: %d%n",
            before - after, after);
        LOG.info("Duplicates removed: {} → {} rows", before, after);
        return result;
    }

    /**
     * Removes rows containing null values.
     *
     * @param df      input DataFrame
     * @param mode    {@code "any"} to drop rows with any null,
     *                {@code "all"} to drop only when all null
     * @param columns columns to inspect for nulls;
     *                pass an empty array to consider all columns
     * @return DataFrame without the matching null rows
     * @throws IllegalArgumentException if any specified column does
     *                                  not exist
     */
    public Dataset<Row> removeNulls(
            final Dataset<Row> df,
            final String mode,
            final String[] columns) {

        long before = df.count();
        Dataset<Row> result;

        if (columns.length == 0) {
            result = df.na().drop(mode);
        } else {
            for (String c : columns) {
                if (!SchemaValidator.columnExists(df, c)) {
                    throw new IllegalArgumentException(
                        "Coluna não encontrada: " + c);
                }
            }
            result = df.na().drop(mode, columns);
        }

        long after = result.count();
        System.out.printf(
            "Linhas com nulos removidas: %d. Total final: %d%n",
            before - after, after);
        LOG.info("Nulls removed (mode={}): {} → {} rows",
            mode, before, after);
        return result;
    }

    /**
     * Casts a column to the specified Spark SQL type.
     *
     * @param df         input DataFrame
     * @param column     column to cast
     * @param targetType Spark SQL type string: {@code "int"},
     *                   {@code "double"}, {@code "string"},
     *                   {@code "date"}, {@code "timestamp"}, etc.
     * @return DataFrame with the column cast to the new type
     * @throws IllegalArgumentException if the column does not exist
     */
    public Dataset<Row> castColumn(
            final Dataset<Row> df,
            final String column,
            final String targetType) {

        if (!SchemaValidator.columnExists(df, column)) {
            throw new IllegalArgumentException(
                "Coluna não encontrada: " + column);
        }

        Dataset<Row> result =
            df.withColumn(column, col(column).cast(targetType));
        LOG.info("Column '{}' cast to type '{}'", column, targetType);
        System.out.printf(
            "Coluna '%s' convertida para tipo '%s'.%n",
            column, targetType);
        return result;
    }

    /**
     * Fills null values in the specified column with a constant string.
     *
     * @param df     input DataFrame
     * @param column column in which nulls should be replaced
     * @param value  string representation of the fill value
     * @return DataFrame with nulls replaced in the given column
     * @throws IllegalArgumentException if the column does not exist
     */
    public Dataset<Row> fillNull(
            final Dataset<Row> df,
            final String column,
            final String value) {

        if (!SchemaValidator.columnExists(df, column)) {
            throw new IllegalArgumentException(
                "Coluna não encontrada: " + column);
        }

        Dataset<Row> result = df.withColumn(column,
            functions.when(col(column).isNull(), functions.lit(value))
                     .otherwise(col(column)));
        LOG.info("Nulls in column '{}' filled with '{}'",
            column, value);
        System.out.printf(
            "Valores nulos na coluna '%s' preenchidos com '%s'.%n",
            column, value);
        return result;
    }

    /**
     * Returns a random sample of the DataFrame.
     *
     * @param df       input DataFrame
     * @param fraction sampling fraction between 0.0 (exclusive) and
     *                 1.0 (inclusive)
     * @param withSeed {@code true} to use a fixed seed for
     *                 reproducibility
     * @return sampled DataFrame
     * @throws IllegalArgumentException if {@code fraction} is not in
     *                                  (0, 1]
     */
    public Dataset<Row> sample(
            final Dataset<Row> df,
            final double fraction,
            final boolean withSeed) {

        if (fraction <= 0.0 || fraction > 1.0) {
            throw new IllegalArgumentException(
                "Fração deve estar entre 0.0 (exclusivo) e 1.0"
                + " (inclusivo). Valor recebido: " + fraction);
        }

        Dataset<Row> result = withSeed
            ? df.sample(false, fraction, SAMPLE_SEED)
            : df.sample(false, fraction);

        LOG.info("Sample applied: fraction={}, withSeed={}",
            fraction, withSeed);
        System.out.printf(
            "Amostragem aplicada: fração=%.2f%n", fraction);
        return result;
    }

    /**
     * Applies a window function and adds the result as a new column.
     *
     * @param df            input DataFrame
     * @param newColumnName name for the new column (must not exist)
     * @param function      window function to apply
     * @param partitionBy   column to partition by, or {@code null}
     *                      for no partition
     * @param orderBy       column to order by within the window
     * @param offset        offset used by {@code LAG} and {@code LEAD}
     * @return DataFrame with the new window column appended
     * @throws IllegalArgumentException if {@code newColumnName} exists,
     *                                  {@code orderBy} does not exist,
     *                                  or {@code partitionBy} (when
     *                                  non-null) does not exist
     */
    public Dataset<Row> applyWindowFunction(
            final Dataset<Row> df,
            final String newColumnName,
            final WindowFunctionType function,
            final String partitionBy,
            final String orderBy,
            final int offset) {

        if (SchemaValidator.columnExists(df, newColumnName)) {
            throw new IllegalArgumentException(
                "Já existe uma coluna com o nome: " + newColumnName);
        }
        if (!SchemaValidator.columnExists(df, orderBy)) {
            throw new IllegalArgumentException(
                "Coluna orderBy não encontrada: " + orderBy);
        }
        if (partitionBy != null && !partitionBy.isEmpty()
                && !SchemaValidator.columnExists(df, partitionBy)) {
            throw new IllegalArgumentException(
                "Coluna partitionBy não encontrada: " + partitionBy);
        }

        WindowSpec window = buildWindowSpec(partitionBy, orderBy);
        Column windowCol =
            buildWindowColumn(function, orderBy, offset, window);

        Dataset<Row> result = df.withColumn(newColumnName, windowCol);
        LOG.info("Window function {} applied as column '{}'",
            function, newColumnName);
        System.out.printf(
            "Coluna '%s' criada com função de janela %s.%n",
            newColumnName, function.getLabel());
        return result;
    }

    private WindowSpec buildWindowSpec(
            final String partitionBy,
            final String orderBy) {
        if (partitionBy != null && !partitionBy.isEmpty()) {
            return Window.partitionBy(col(partitionBy))
                         .orderBy(col(orderBy));
        }
        return Window.orderBy(col(orderBy));
    }

    private Column buildWindowColumn(
            final WindowFunctionType function,
            final String orderBy,
            final int offset,
            final WindowSpec window) {

        switch (function) {
            case RANK:
                return functions.rank().over(window);
            case DENSE_RANK:
                return functions.dense_rank().over(window);
            case ROW_NUMBER:
                return functions.row_number().over(window);
            case LAG:
                return functions.lag(orderBy, offset).over(window);
            case LEAD:
                return functions.lead(orderBy, offset).over(window);
            default:
                throw new IllegalArgumentException(
                    "Função de janela não suportada: " + function);
        }
    }

    private Object parseValue(final String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
