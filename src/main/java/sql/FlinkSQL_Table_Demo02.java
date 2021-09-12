package sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 需求：
 * 使用SQL和Table两种方式对DataStream中的单词进行统计
 */
public class FlinkSQL_Table_Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStream<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );

        tableEnvironment.createTemporaryView("WordCount", input, $("word"), $("frequency"));

        //sql
        Table resultTable = tableEnvironment.sqlQuery(
                "select word, sum(frequency) as frequency from WordCount group by word"
        );

        //table
        Table table = tableEnvironment.fromDataStream(input);
        Table table1 = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));

        DataStream<Tuple2<Boolean, WC>> resultDS = tableEnvironment.toRetractStream(resultTable, WC.class);
        DataStream<Tuple2<Boolean, WC>> resultDS2 = tableEnvironment.toRetractStream(table1, WC.class);

        resultDS.print();
        resultDS2.print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WC{
        private String word;
        private long frequency;
    }
}
