package windows;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 有如下数据表示:
 * 信号灯编号和通过该信号灯的车的数量
 9,3
 9,2
 9,7
 4,9
 2,6
 1,5
 2,3
 5,7
 5,4
 * 需求1:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
 * 需求2:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计--基于数量的滑动窗口
 */

public class WindowDemo02_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketTextStream = env.socketTextStream("192.168.200.101", 9999);

        // 9, 3 ==> (9, 3)
        SingleOutputStreamOperator<CarInfo> carInfoDS = socketTextStream.map(new MapFunction<String, CarInfo>() {
            @Override
            public CarInfo map(String value) throws Exception {
                String[] strings = value.split(",");
                return new CarInfo(strings[0], Integer.parseInt(strings[1]));
            }
        });

        //需求1:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
        SingleOutputStreamOperator<CarInfo> result1 = carInfoDS.keyBy(CarInfo::getId)
                .countWindow(5L)
                .sum("count");

        //需求2:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计--基于数量的滑动窗口
        SingleOutputStreamOperator<CarInfo> result2 = carInfoDS.keyBy(CarInfo::getId)
                .countWindow(5L, 3L)
                .sum("count");

        result1.print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CarInfo{
        private String id;
        private Integer count;
    }
}
