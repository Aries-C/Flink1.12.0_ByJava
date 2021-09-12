package sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 需求：
 * 将 Datastream 注册为 Table 和 View 并进行 SQL 统计
 */
public class FlinkSQL_Table_Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)

        ));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)
        ));

        //注册表
        Table tableA = tableEnvironment.fromDataStream(orderA, $("userId"), $("product"), $("amount"));
        tableEnvironment.createTemporaryView("OrderB", orderB, $("userId"), $("product"), $("amount"));

        //执行查询
        System.out.println(tableA);
        Table resultTable = tableEnvironment.sqlQuery(
                "SELECT * FROM " + tableA + " WHERE amount > 2 " +
                        "UNION ALL " +
                        "SELECT * FROM OrderB WHERE amount < 2"
        );

        //输出
        DataStream<Order> resultDS = tableEnvironment.toAppendStream(resultTable, Order.class);
        resultDS.print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order{
        private Long userId;
        private String product;
        private int amount;
    }
}
