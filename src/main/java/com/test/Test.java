package com.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * Basic example of generating data and printing it.
 */
public class Test {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createFunction("toLager", ToLager.class);
        tableEnv.createFunction("toPreis", ToPreis.class);

        DataStream<Row> bestandStream = env.fromElements(
                Row.of("11111", "222222", "lager1", 1),
                Row.of("11111", "222222", "lager2", 2),
                Row.of("11112", "222223", "lager1", 5));

        DataStream<Row> preisStream = env.fromElements(
                Row.of("11111", "222222", "3333333", 10.5),
                Row.of("11111", "222222", "3333334", 9.99),
                Row.of("11111", "222223", "3333333",  20.0),
                Row.of("11112", "222222", "3333333",  49.99));

        DataStream<Row> zuweisungStream = env.fromElements(
                Row.of("11111", "222222"),
                Row.of("11111", "222223"),
                Row.of("11112", "222222"));

        Table bestand = tableEnv.fromDataStream(bestandStream).as("artikelNummer", "partnerNummer", "lager", "menge");
        tableEnv.createTemporaryView("bestand", bestand);

        Table preis = tableEnv.fromDataStream(preisStream).as("artikelNummer", "partnerNummer", "lieferant", "wert");
        tableEnv.createTemporaryView("preis", preis);

        Table zuweisung = tableEnv.fromDataStream(zuweisungStream).as("artikelNummer", "partnerNummer");
        tableEnv.createTemporaryView("zuweisung", zuweisung);

        Table resultTable = tableEnv.sqlQuery(
            "SELECT " +
                    "zuweisung.artikelNummer, " +
                    "zuweisung.partnerNummer, " +
                    "collect(toLager(bestand.lager, bestand.menge)) as bestand, " +
                    "collect(toPreis(preis.lieferant, preis.wert)) as preis " +
                    "from zuweisung " +
                    "join preis on zuweisung.artikelNummer = preis.artikelNummer and zuweisung.partnerNummer = preis.partnerNummer " +
                    "join bestand on zuweisung.artikelNummer = bestand.artikelNummer and zuweisung.partnerNummer = preis.partnerNummer " +
                    "group by zuweisung.artikelNummer, zuweisung.partnerNummer"
        );

        resultTable.execute().print();
    }
}
