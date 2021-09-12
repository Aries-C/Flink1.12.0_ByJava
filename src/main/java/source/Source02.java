package source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Desc
 * env.readTextFile(本地/HDFS文件/文件夹);//压缩文件也可以
 */
public class Source02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds1 = env.readTextFile("data\\data1.txt");
        DataStream<String> ds2 = env.readTextFile("hdfs://flink01:8020/input/test.txt");
        DataStream<String> ds3 = env.readTextFile("data");
        DataStream<String> ds4 = env.readTextFile("datazip\\zipdata.gz");

        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        env.execute();
    }
}
