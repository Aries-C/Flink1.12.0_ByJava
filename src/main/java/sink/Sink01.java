package sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sink01 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Long> ds = env.fromSequence(1, 10);
        ds.print();
        ds.printToErr();
        ds.writeAsText("./result/sink01.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();
    }
}
