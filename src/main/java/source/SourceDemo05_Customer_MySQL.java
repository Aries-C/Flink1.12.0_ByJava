package source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Desc
 * 需求:
 * 实际开发中,经常会实时接收一些数据,要和MySQL中存储的一些规则进行匹配,那么这时候就可以使用Flink自定义数据源从MySQL中读取数据
 * 那么现在先完成一个简单的需求:
 * 从MySQL中实时加载数据
 * 要求MySQL中的数据有变化,也能被实时加载出来
 */
public class SourceDemo05_Customer_MySQL {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        DataStream<Student> studentDS = env.addSource(new MySQLSource()).setParallelism(1);

        //3.Transformation
        //4.Sink
        studentDS.print();

        //5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSource extends RichParallelSourceFunction<Student> {
        private Connection conn = null;
        private PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动,开启连接
            //Class.forName("com.mysql.jdbc.Driver");
            //Caused by: javax.net.ssl.SSLHandshakeException: No appropriate protocol (protocol is disabled or cipher suites are inappropriate)
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test", "root", "000000");
            String sql = "select id,name,age from t_student";
            ps = conn.prepareStatement(sql);
        }

        private boolean flag = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (flag) {
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    ctx.collect(new Student(id, name, age));
                }
                TimeUnit.SECONDS.sleep(5);
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }
    }
}

