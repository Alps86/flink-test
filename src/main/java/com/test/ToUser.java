package com.test;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ToUser extends ScalarFunction {
    @DataTypeHint("ROW<name STRING, socre INT>")
    public Row eval(String name, Integer score) {
        return Row.of(name, score);
    }
}
