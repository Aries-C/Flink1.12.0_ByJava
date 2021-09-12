package windows;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 需求1:
 * 每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量
 * --基于时间的滚动窗口
 * 需求2:
 * 每5秒钟统计一次，最近10秒钟内，各个路口/信号灯通过红绿灯汽车的数量
 * --基于时间的滑动窗口
 * （信号灯编号， 通过数量）
 * 9,3
 * 9,2
 * 9,7
 * 4,9
 * 2,6
 * 1,5
 * 2,3
 * 5,7
 * 5,4
 */
public class WindowDemo01_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketTextStream = env.socketTextStream("192.168.200.101", 9999);

        // 9, 3 ==> (9, 3)
        SingleOutputStreamOperator<CityInfo> carInfoDS = socketTextStream.map(new MapFunction<String, CityInfo>() {
            @Override
            public CityInfo map(String value) throws Exception {
                String[] strings = value.split(",");
                return new CityInfo(strings[0], Integer.parseInt(strings[1]));
            }
        });

        //每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
        SingleOutputStreamOperator<CityInfo> result1 = carInfoDS.keyBy(CityInfo::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("count");

        // * 每5秒钟统计一次，最近10秒钟内，各个路口/信号灯通过红绿灯汽车的数量--基于时间的滑动窗口
        SingleOutputStreamOperator<CityInfo> result2 = carInfoDS.keyBy(CityInfo::getId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count");

        result1.print();

        env.execute();
    }

    public static class CityInfo{
        private String id;
        private Integer count;

        public CityInfo() {
        }

        public CityInfo(String id, Integer count) {
            this.id = id;
            this.count = count;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "路口：" + this.id + ", 车辆：" + this.count;
        }
    }
}
