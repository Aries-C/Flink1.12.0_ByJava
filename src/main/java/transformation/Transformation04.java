package transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Transformation04 {
    public static void main(String[] args) throws Exception {
        /**
         * rebalance(内部使用round robin方法将数据均匀打散)
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Long> longDS = env.fromSequence(0, 100);

        //将数据随机分配，模拟数据倾斜
        SingleOutputStreamOperator<Long> filterDS = longDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 10;
            }
        });


        //使用 map 操作， 将数据转换为（分区编号/子任务编号， 数据）
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDS.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                //获取分区编号/子任务编号
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDS.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        result1.print("result1:");
        result2.print("result2:");

        env.execute();
    }
}
