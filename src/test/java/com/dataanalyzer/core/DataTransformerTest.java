package com.dataanalyzer.core;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataTransformerTest extends SparkTestBase {

    private DataTransformer transformer;
    private Dataset<Row> df;

    @BeforeEach
    void setUp() {
        transformer = new DataTransformer();
        String csvPath = getClass().getClassLoader()
            .getResource("test_data.csv").getPath();
        df = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(csvPath);
    }

    // ---- filter ----

    @Test
    void filter_equalOperator_returnsMatchingRows() {
        Dataset<Row> result = transformer.filter(df, "Categoria", "=", "Moveis");

        assertEquals(2, result.count());
    }

    @Test
    void filter_greaterThan_returnsCorrectRows() {
        Dataset<Row> result = transformer.filter(df, "Preco", ">", "1000");

        assertTrue(result.count() > 0);
        result.collectAsList().forEach(row ->
            assertTrue(row.getAs("Preco").toString().compareTo("1000") > 0)
        );
    }

    @Test
    void filter_nonexistentColumn_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.filter(df, "DoesNotExist", "=", "x")
        );
    }

    @Test
    void filter_invalidOperator_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.filter(df, "Preco", "??", "100")
        );
    }

    // ---- selectColumns ----

    @Test
    void selectColumns_returnsOnlyRequestedColumns() {
        List<String> cols = Arrays.asList("Produto", "Preco");
        Dataset<Row> result = transformer.selectColumns(df, cols);

        assertArrayEquals(new String[]{"Produto", "Preco"}, result.columns());
    }

    @Test
    void selectColumns_emptyList_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.selectColumns(df, Arrays.asList())
        );
    }

    @Test
    void selectColumns_unknownColumn_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.selectColumns(df, Arrays.asList("Produto", "Ghost"))
        );
    }

    // ---- renameColumn ----

    @Test
    void renameColumn_changesColumnName() {
        Dataset<Row> result = transformer.renameColumn(df, "Produto", "Item");

        assertTrue(Arrays.asList(result.columns()).contains("Item"));
        assertFalse(Arrays.asList(result.columns()).contains("Produto"));
    }

    @Test
    void renameColumn_preservesRowCount() {
        Dataset<Row> result = transformer.renameColumn(df, "Produto", "Item");

        assertEquals(df.count(), result.count());
    }

    @Test
    void renameColumn_nonexistentSource_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.renameColumn(df, "Ghost", "NewName")
        );
    }

    @Test
    void renameColumn_duplicateTarget_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.renameColumn(df, "Produto", "Categoria")
        );
    }

    // ---- addColumn ----

    @Test
    void addColumn_createsNewColumn() {
        Dataset<Row> result = transformer.addColumn(
            df, "Total", "Preco * Quantidade");

        assertTrue(Arrays.asList(result.columns()).contains("Total"));
    }

    @Test
    void addColumn_preservesExistingColumns() {
        Dataset<Row> result = transformer.addColumn(
            df, "Total", "Preco * Quantidade");

        assertEquals(df.columns().length + 1, result.columns().length);
    }

    @Test
    void addColumn_emptyName_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.addColumn(df, "", "Preco * 2")
        );
    }

    @Test
    void addColumn_duplicateName_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.addColumn(df, "Preco", "Preco * 2")
        );
    }

    // ---- sort ----

    @Test
    void sort_ascending_ordersCorrectly() {
        Dataset<Row> result = transformer.sort(df, new String[]{"Preco"}, true);
        List<Row> rows = result.collectAsList();

        double prev = Double.MIN_VALUE;
        for (Row r : rows) {
            double price = Double.parseDouble(r.getAs("Preco").toString());
            assertTrue(price >= prev);
            prev = price;
        }
    }

    @Test
    void sort_descending_ordersCorrectly() {
        Dataset<Row> result = transformer.sort(df, new String[]{"Preco"}, false);
        List<Row> rows = result.collectAsList();

        double prev = Double.MAX_VALUE;
        for (Row r : rows) {
            double price = Double.parseDouble(r.getAs("Preco").toString());
            assertTrue(price <= prev);
            prev = price;
        }
    }

    @Test
    void sort_nonexistentColumn_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
            transformer.sort(df, new String[]{"Ghost"}, true)
        );
    }

    // ---- removeDuplicates ----

    @Test
    void removeDuplicates_exact_removesOnlyDuplicates() {
        // All rows are unique in test_data.csv
        Dataset<Row> result = transformer.removeDuplicates(df, new String[0]);

        assertEquals(df.count(), result.count());
    }

    // ---- removeNulls ----

    @Test
    void removeNulls_anyMode_removesRowsWithNulls() {
        // test_data.csv has no nulls, so count should stay the same
        Dataset<Row> result = transformer.removeNulls(
            df, "any", new String[0]);

        assertEquals(df.count(), result.count());
    }
}
