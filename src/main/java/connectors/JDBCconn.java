package connectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class JDBCconn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> source = env.fromElements(new Student(null, "tonym", 18));

        source.addSink(JdbcSink.sink(
                "insert into t_student (id, name, age) values(null, ?, ?)",
                (ps, s) -> {
                    ps.setString(1, s.getName());
                    ps.setInt(2, s.getAge());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/flink_test")
                .withUsername("root")
                .withPassword("000000")
                .withDriverName("com.mysql.jdbc.Driver")
                .build()
        ));

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student{
        private Integer id;
        private String name;
        private Integer age;
    }
}
