package transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Transformation03 {
    public static void main(String[] args) throws Exception {
        /**
         * 对流中的数据按照奇数和偶数进行分流，并获取分流后的数据
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //定义两个输出标签
        OutputTag<Integer> tag_even = new OutputTag<>("偶数", TypeInformation.of(Integer.class));
        OutputTag<Integer> tag_odd = new OutputTag<Integer>("奇数"){};

        SingleOutputStreamOperator<Integer> tagResult = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(tag_even, value);
                } else {
                    ctx.output(tag_odd, value);
                }
            }
        });

        //取出标记好的数据
        DataStream<Integer> evenResult = tagResult.getSideOutput(tag_even);
        DataStream<Integer> oddResult = tagResult.getSideOutput(tag_odd);

        evenResult.print("偶数：");
        oddResult.print("奇数：");

        env.execute();
    }
}
