package source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class Source05 {
    /**
     * 使用Flink自定义数据源从MySQL中读取数据
     * 那么现在先完成一个简单的需求:
     * 从MySQL中实时加载数据
     * 要求MySQL中的数据有变化,也能被实时加载出来
     */
    /**
     * CREATE TABLE `t_student` (
     *     `id` int(11) NOT NULL AUTO_INCREMENT,
     *     `name` varchar(255) DEFAULT NULL,
     *     `age` int(11) DEFAULT NULL,
     *     PRIMARY KEY (`id`)
     * ) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
     *
     * INSERT INTO `t_student` VALUES ('1', 'jack', '18');
     * INSERT INTO `t_student` VALUES ('2', 'tom', '19');
     * INSERT INTO `t_student` VALUES ('3', 'rose', '20');
     * INSERT INTO `t_student` VALUES ('4', 'tom', '19');
     * INSERT INTO `t_student` VALUES ('5', 'jack', '18');
     * INSERT INTO `t_student` VALUES ('6', 'rose', '20');
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> studentDS = env.addSource(new MySQLSource()).setParallelism(1);
        studentDS.print();
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
    public static class MySQLSource extends RichParallelSourceFunction<Student>{
        private Connection conn = null;
        private PreparedStatement ps = null;
        private boolean flag = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test", "root", "000000");
            String sql = "select id, name, age from t_student";
            ps = conn.prepareStatement(sql);
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
