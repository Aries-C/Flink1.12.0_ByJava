package action;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * 需求：
 * 在订单完成之后，一定时间之内没有做出评价，系统自动给与五星好评
 */
public class OrderAutomaticFavorableComments {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String, String, Long>> sourceDS = env.addSource(new MySource());

        //经过interval用户未对订单做出评价，自动给与好评.
        long interval = 5000L;
        sourceDS.keyBy(0)
                .process(new TimeProcessFuntion(interval));

        env.execute();
    }
    /**
     * 自定义source实时产生订单数据Tuple2<订单id, 订单生成时间>
     */
    public static class MySource implements SourceFunction<Tuple3<String, String, Long>> {
        private boolean flag = true;
        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                String userId = random.nextInt(5) + "";
                String orderId = UUID.randomUUID().toString();
                long currentTimeMillis = System.currentTimeMillis();
                ctx.collect(Tuple3.of(userId, orderId, currentTimeMillis));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    /**
     * 自定义处理函数用来给超时订单做自动好评!
     * 如一个订单进来:<订单id, 2020-10-10 12:00:00>
     * 那么该订单应该在12:00:00 + 5s 的时候超时!
     * 所以我们可以在订单进来的时候设置一个定时器,在订单时间 + interval的时候触发!
     * KeyedProcessFunction<K, I, O>
     * KeyedProcessFunction<Tuple就是String, Tuple3<用户id, 订单id, 订单生成时间>, Object>
     */
    public static class TimeProcessFuntion extends KeyedProcessFunction<Tuple, Tuple3<String, String, Long>, Object> {
        private long interval;

        public TimeProcessFuntion(long interval) {
            this.interval = interval;
        }

        //定义MapState类型的状态，key是订单号， value是订单完成时间
        //定义一个状态用来记录订单信息
        private MapState<String, Long> mapState;

        //初始化MapState


        @Override
        public void open(Configuration parameters) throws Exception {
            //创建状态描述器
            MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<>("mapState", String.class, Long.class);
            //根据状态描述器初始化状态
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        //注册定时器，处理每一个订单并设置定时器
        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            mapState.put(value.f1, value.f2);
            //如一个订单进来:<订单id, 2020-10-10 12:00:00>
            //那么该订单应该在12:00:00 + 5s 的时候超时!
            //在订单进来的时候设置一个定时器,在订单时间 + interval的时候触发!!!
            ctx.timerService().registerProcessingTimeTimer(value.f2 + interval);
        }

        //在生产环境下，可以去查询相关的订单系统
        private boolean isEvaluation(String key) {
            return key.hashCode() % 2 == 0;//随机返回订单是否已评价
        }


        //定时器被触发时执行并输出结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            //能够执行到这里说明订单超时了!超时了得去看看订单是否评价了(实际中应该要调用外部接口/方法查订单系统!,我们这里没有,所以模拟一下)
            //没有评价才给默认好评!并直接输出提示!
            //已经评价了,直接输出提示!
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                String orderId = entry.getKey();
                //调用订单系统查询是否已经评价
                boolean result = isEvaluation(orderId);
                if (result) {//已评价
                    System.out.println("订单(orderid: " + orderId + ")在" + interval + "毫秒时间内已经评价，不做处理");
                } else {//未评价
                    System.out.println("订单(orderid: " + orderId + ")在" + interval + "毫秒时间内未评价，系统自动给了默认好评!");
                    //实际中还需要调用订单系统将该订单orderId设置为5星好评!
                }
                //从状态中移除已经处理过的订单,避免重复处理
                iterator.remove();
            }
        }
    }
}
