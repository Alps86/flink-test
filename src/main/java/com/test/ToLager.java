package com.test;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ToLager extends ScalarFunction {
    @DataTypeHint("ROW<name STRING, lager INT>")
    public Row eval(String name, Integer bestand) {
        return Row.of(name, bestand);
    }
}
