package source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desc
 * env.readTextFile(本地/HDFS文件/文件夹);//压缩文件也可以
 */
public class Source03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.socketTextStream("flink01", 9999);

        SingleOutputStreamOperator<String> flatMap = ds1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyBy = map.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        sum.print();

        env.execute();
    }
}
