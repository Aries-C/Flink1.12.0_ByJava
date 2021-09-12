package source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Desc
 * 把本地的普通的Java集合/Scala集合变为分布式的Flink的DataStream集合!
 * 一般用于学习测试时编造数据时使用
 * 1.env.fromElements(可变参数);
 * 2.env.fromColletion(各种集合);
 * 3.env.generateSequence(开始,结束);
 * 4.env.fromSequence(开始,结束);
 */
public class Source01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // * 1.env.fromElements(可变参数);
        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "flink");

        // * 2.env.fromColletion(各种集合);
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("hadoop", "spark", "flink"));

        // * 3.env.generateSequence(开始,结束);
        DataStream<Long> ds3 = env.generateSequence(1, 10);

        // * 4.env.fromSequence(开始,结束);
        DataStream<Long> ds4 = env.fromSequence(1, 10);


        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        env.execute();
    }
}
