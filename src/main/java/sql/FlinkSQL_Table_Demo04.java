package sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 需求：
 * 从Kafka中消费数据并过滤出状态为success的数据再写入到Kafka
 *
 * {"user_id": "1", "page_id":"1", "status": "success"}
 * {"user_id": "1", "page_id":"1", "status": "success"}
 * {"user_id": "1", "page_id":"1", "status": "success"}
 * {"user_id": "1", "page_id":"1", "status": "success"}
 * {"user_id": "1", "page_id":"1", "status": "fail"}
 *
 * /export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic input_kafka
 * /export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic output_kafka
 * /export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic input_kafka
 * /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic output_kafka --from-beginning
 */
public class FlinkSQL_Table_Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TableResult inputTable = tableEnv.executeSql(
                "create table input_kafka(\n" +
                        "user_id bigint,\n" +
                        "page_id bigint,\n" +
                        "status string\n" +
                        ") with (\n" +
                        "'connector' = 'kafka',\n" +
                        "'topic' = 'input_kafka',\n" +
                        "'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "'properties.group.id' = 'testGroup',\n" +
                        "'scan.startup.mode' = 'latest-offset',\n" +
                        "'format' = 'json'\n" +
                        ")"
        );

        TableResult outputTable = tableEnv.executeSql(
                "CREATE TABLE output_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'output_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +
                        ")"
        );

        String sql = "select user_id, page_id, status\n" +
                "from input_kafka\n" +
                "where status = 'success'";

        Table resultTable = tableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);

        tableEnv.executeSql("insert into output_kafka select * from " + resultTable);


        env.execute();
    }
}
