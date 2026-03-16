package com.dataanalyzer.util;

import com.dataanalyzer.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaValidatorTest extends SparkTestBase {

    private Dataset<Row> df;

    @BeforeEach
    void buildDataFrame() {
        StructType schema = new StructType()
            .add("name",  DataTypes.StringType,  false)
            .add("price", DataTypes.DoubleType,  false)
            .add("qty",   DataTypes.IntegerType, false);

        df = spark.createDataFrame(
            java.util.Arrays.asList(
                RowFactory.create("Notebook", 2500.0, 2),
                RowFactory.create("Mouse",     50.0, 10)
            ),
            schema
        );
    }

    @Test
    void columnExists_returnsTrue_whenColumnPresent() {
        assertTrue(SchemaValidator.columnExists(df, "name"));
    }

    @Test
    void columnExists_returnsFalse_whenColumnAbsent() {
        assertFalse(SchemaValidator.columnExists(df, "nonexistent"));
    }

    @Test
    void allColumnsExist_returnsTrue_whenAllPresent() {
        assertTrue(SchemaValidator.allColumnsExist(df, "name", "price"));
    }

    @Test
    void allColumnsExist_returnsFalse_whenOneAbsent() {
        assertFalse(SchemaValidator.allColumnsExist(df, "name", "missing"));
    }

    @Test
    void isNumeric_returnsTrue_forDoubleColumn() {
        assertTrue(SchemaValidator.isNumeric(df, "price"));
    }

    @Test
    void isNumeric_returnsTrue_forIntegerColumn() {
        assertTrue(SchemaValidator.isNumeric(df, "qty"));
    }

    @Test
    void isNumeric_returnsFalse_forStringColumn() {
        assertFalse(SchemaValidator.isNumeric(df, "name"));
    }

    @Test
    void isNumeric_returnsFalse_forNonexistentColumn() {
        assertFalse(SchemaValidator.isNumeric(df, "ghost"));
    }
}
