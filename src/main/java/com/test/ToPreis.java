package com.test;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class ToPreis extends ScalarFunction {
    @DataTypeHint("ROW<partnerNummer STRING, preis DOUBLE>")
    public Row eval(String name, Double preis) {
        return Row.of(name, preis);
    }
}
