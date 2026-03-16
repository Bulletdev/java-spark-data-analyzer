package com.dataanalyzer.core;

import com.dataanalyzer.util.SchemaValidator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
 * State is never held internally, which makes these methods easy to test
 * in isolation.
 */
public class DataTransformer {

    private static final Logger log =
        LoggerFactory.getLogger(DataTransformer.class);

    /**
     * Filters the DataFrame using a simple column comparison.
     *
     * @param df       input DataFrame
     * @param column   column name to filter on
     * @param operator comparison operator: {@code =, >, <, >=, <=, !=}
     * @param value    value to compare against (numeric or string)
     * @return filtered DataFrame
     * @throws IllegalArgumentException if the column does not exist
     *                                  or the operator is not recognised
     */
    public Dataset<Row> filter(
            Dataset<Row> df, String column, String operator, String value) {

        if (!SchemaValidator.columnExists(df, column)) {
            throw new IllegalArgumentException(
                "Coluna não encontrada: " + column);
        }

        Column colRef = functions.col(column);
        Object filterValue = parseValue(value);

        Dataset<Row> filtered;
        switch (operator) {
            case "=":  filtered = df.filter(colRef.equalTo(filterValue)); break;
            case ">":  filtered = df.filter(colRef.gt(filterValue));      break;
            case "<":  filtered = df.filter(colRef.lt(filterValue));      break;
            case ">=": filtered = df.filter(colRef.geq(filterValue));     break;
            case "<=": filtered = df.filter(colRef.leq(filterValue));     break;
            case "!=": filtered = df.filter(colRef.notEqual(filterValue)); break;
            default:
                throw new IllegalArgumentException(
                    "Operador inválido: " + operator
                    + ". Válidos: =, >, <, >=, <=, !=");
        }

        long count = filtered.count();
        System.out.println("Filtro aplicado. Linhas resultantes: " + count);
        log.info("Filter: {} {} {} → {} rows", column, operator, value, count);
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
    public Dataset<Row> selectColumns(Dataset<Row> df, List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Nenhuma coluna especificada.");
        }
        for (String c : columns) {
            if (!SchemaValidator.columnExists(df, c)) {
                throw new IllegalArgumentException(
                    "Coluna não encontrada: " + c);
            }
        }

        String first = columns.get(0);
        String[] rest = columns.subList(1, columns.size()).toArray(new String[0]);
        Dataset<Row> result = rest.length == 0
            ? df.select(first)
            : df.select(first, rest);

        log.info("Columns selected: {}", columns);
        return result;
    }

    /**
     * Renames a column in the DataFrame.
     *
     * @param df      input DataFrame
     * @param oldName current name of the column
     * @param newName desired new name
     * @return DataFrame with the column renamed
     * @throws IllegalArgumentException if the old column does not exist,
     *                                  the new name is blank, or it already exists
     */
    public Dataset<Row> renameColumn(
            Dataset<Row> df, String oldName, String newName) {

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
        System.out.println("Coluna '" + oldName + "' renomeada para '"
            + newName + "'.");
        log.info("Column renamed: {} → {}", oldName, newName);
        return result;
    }

    /**
     * Adds a new column derived from a Spark SQL expression.
     *
     * @param df            input DataFrame
     * @param newColumnName name for the new column
     * @param expression    Spark SQL expression (e.g. {@code "Preco * Quantidade"})
     * @return DataFrame with the new column appended
     * @throws IllegalArgumentException if the name is blank, already exists,
     *                                  or the expression is empty
     */
    public Dataset<Row> addColumn(
            Dataset<Row> df, String newColumnName, String expression) {

        if (newColumnName == null || newColumnName.isEmpty()) {
            throw new IllegalArgumentException(
                "Nome da nova coluna não pode ser vazio.");
        }
        if (SchemaValidator.columnExists(df, newColumnName)) {
            throw new IllegalArgumentException(
                "Já existe uma coluna com o nome: " + newColumnName);
        }
        if (expression == null || expression.isEmpty()) {
            throw new IllegalArgumentException("Expressão não pode ser vazia.");
        }

        Dataset<Row> result = df.withColumn(newColumnName, expr(expression));
        System.out.println("Coluna '" + newColumnName + "' criada.");
        log.info("Column added: {} = {}", newColumnName, expression);
        return result;
    }

    /**
     * Sorts the DataFrame by the given columns.
     *
     * @param df          input DataFrame
     * @param columnNames columns to sort by
     * @param ascending   {@code true} for ascending, {@code false} for descending
     * @return sorted DataFrame
     * @throws IllegalArgumentException if any column does not exist
     */
    public Dataset<Row> sort(
            Dataset<Row> df, String[] columnNames, boolean ascending) {

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
        log.info("Data sorted by {} (ascending={})",
            Arrays.toString(columnNames), ascending);
        return result;
    }

    /**
     * Removes duplicate rows from the DataFrame.
     *
     * @param df         input DataFrame
     * @param keyColumns columns to use as deduplication key;
     *                   pass an empty array for exact-row deduplication
     * @return deduplicated DataFrame
     * @throws IllegalArgumentException if any key column does not exist
     */
    public Dataset<Row> removeDuplicates(Dataset<Row> df, String[] keyColumns) {
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
        log.info("Duplicates removed: {} → {} rows", before, after);
        return result;
    }

    /**
     * Removes rows containing null values.
     *
     * @param df      input DataFrame
     * @param mode    {@code "any"} to drop rows with any null,
     *                {@code "all"} to drop only when all values are null
     * @param columns columns to inspect for nulls;
     *                pass an empty array to consider all columns
     * @return DataFrame without the matching null rows
     * @throws IllegalArgumentException if any specified column does not exist
     */
    public Dataset<Row> removeNulls(
            Dataset<Row> df, String mode, String[] columns) {

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
        log.info("Nulls removed (mode={}): {} → {} rows", mode, before, after);
        return result;
    }

    private Object parseValue(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
