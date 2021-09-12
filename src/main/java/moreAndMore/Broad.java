package moreAndMore;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broad {
    /**
     * 1. 广播数据
     * 2. 获取广播数据
     * 3. 使用广播数据
     *
     * 需求：
     * 将 Student（学号， 姓名）集合广播出去（广播到各个TaskManager中）
     * 然后使用 scoreDS（学号， 学科， 成绩）和广播数据进行关联， 得到新的数据： （姓名， 学科， 成绩）
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, String>> studentDS = env.fromCollection(Arrays.asList(Tuple2.of(1, "zhangsan"), Tuple2.of(2, "lisi"), Tuple2.of(3, "wangwu")));
        DataStream<Tuple3<Integer, String, Integer>> sourceDS = env.fromCollection(Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86)));

        sourceDS.map(
                new RichMapFunction<Tuple3<Integer, String, Integer>, Object>() {
                    //定义一个集合用来存储（学号，姓名）
                    Map<Integer, String> studentMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //-2.获取广播数据
                        List<Tuple2<Integer, String>> studentList = getRuntimeContext().getBroadcastVariable("studentInfo");
                        for (Tuple2<Integer, String> tuple : studentList) {
                            studentMap.put(tuple.f0, tuple.f1);
                        }
                        //studentMap = studentList.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1));
                    }

                    @Override
                    public Object map(Tuple3<Integer, String, Integer> value) throws Exception {
                        //使用广播数据
                        Integer studentId = value.f0;
                        String studentName = studentMap.getOrDefault(studentId, "");
                        return Tuple3.of(studentName, value.f1, value.f2);
                    }
                });
//                .withBroadcastSet(studentDS, "studentInfo");


    }
}
