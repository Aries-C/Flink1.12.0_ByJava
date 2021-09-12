package watermaker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;


/**
 * 有订单数据,格式为: (订单ID，用户ID，时间戳/事件时间，订单金额)
 * 要求每隔5s,计算5秒内，每个用户的订单总金额
 * 并添加Watermaker来解决一定程度上的数据延迟和数据乱序问题。
 * 并使用OutputTag+allowedLateness解决数据丢失问题
 */
public class WatermakerDemo03_AllowedLateness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;
            // source --> transform --> sink
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(100);
                    //模拟数据延迟和乱序!
                    long eventTime = System.currentTimeMillis() - random.nextInt(10) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));

                    //TimeUnit.SECONDS.sleep(1);
                }
            }
            @Override
            public void cancel() {
                flag = false;
            }
        });


        SingleOutputStreamOperator<Order> watermakerDS = orderDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                                Duration.ofSeconds(3)
                        ).withTimestampAssigner((event, timestamp) -> event.getEventTime())
                );

        OutputTag<Order> outputTag = new OutputTag<>("Seriouslylate", TypeInformation.of(Order.class));

        SingleOutputStreamOperator<Order> result = watermakerDS.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .sum("money");

        DataStream<Order> result2 = result.getSideOutput(outputTag);

        result.print("正常数据和迟到不严重的数据：");
        result2.print("迟到严重的数据：");

        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
