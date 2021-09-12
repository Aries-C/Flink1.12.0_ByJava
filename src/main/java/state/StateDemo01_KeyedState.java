package state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateDemo01_KeyedState {
    /**
     * 使用KeyState中的ValueState获取数据中的最大值
     * (实际中直接使用maxBy即可)
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> sourceDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );
        //实际中直接使用maxBy即可
        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = sourceDS.keyBy(t -> t.f0).maxBy(1);

        //另一种实现方式：使用KeyedState中的valueState
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result2 = sourceDS.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    private ValueState<Long> maxValueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态描述符：描述状态里面放入名称和里面放入数据
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor("maxValueState", Long.class);
                        //根据状态描述符初始化状态
                        maxValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                        //使用state，取出state最大值/历史最大值
                        Long historyMaxValue = maxValueState.value();
                        Long currentValue = value.f1;
                        if (historyMaxValue == null || currentValue > historyMaxValue) {
                            //更新状态，把当前的作为新的最大值存到状态中
                            maxValueState.update(currentValue);
                            return Tuple3.of(value.f0, currentValue, currentValue);
                        } else {
                            return Tuple3.of(value.f0, currentValue, historyMaxValue);
                        }
                    }
                });

        result1.print();
        result2.print();

        env.execute();
    }

}
