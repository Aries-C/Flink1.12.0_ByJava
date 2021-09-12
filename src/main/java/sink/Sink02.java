package sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink02 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        /**
         * 将Flink集合中的数据通过自定义Sink保存到MySQL
         */
        DataStreamSource<Student> tony = env.fromElements(new Student(null, "tony", 20));
        tony.addSink(new MySQLSink());

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

    public static class MySQLSink extends RichSinkFunction<Student>{
        private Connection conn = null;
        private PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动，开启连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test?serverTimezone=Asia/Shanghai", "root", "000000");
            String sql = "insert into t_student (id, name, age) values(null, ?, ?)";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Student value, Context context) throws Exception {
            ps.setString(1, value.getName());
            ps.setInt(2, value.getAge());
            ps.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }
    }
}
