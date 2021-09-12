package moreAndMore;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Acc {
    /**
     * 统计处理的条数
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataDS = env.fromElements("aaa", "bbb", "ccc", "ddd");

        SingleOutputStreamOperator<String> result = dataDS.map(new RichMapFunction<String, String>() {
            //创建累加器
            private IntCounter elementCounter = new IntCounter();
            Integer count = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //注册累加器
                getRuntimeContext().addAccumulator("elementCounter", elementCounter);
            }

            @Override
            public String map(String value) throws Exception {
                //使用累加器
                this.elementCounter.add(1);
                count += 1;
                System.out.println("不使用累加器统计的结果：" + count);
                return value;
            }
        }).setParallelism(2);

        result.writeAsText("data/output/test", FileSystem.WriteMode.OVERWRITE);

        //获取加强结果
        JobExecutionResult jobResult = env.execute();
        int nums = jobResult.getAccumulatorResult("elementCounter");
        System.out.println("使用累加器计算结果：" + nums);
    }
}
