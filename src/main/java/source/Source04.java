package source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

public class Source04 {
    public static void main(String[] args) throws Exception {
        /**
         * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
         * 要求:
         * - 随机生成订单ID(UUID)
         * - 随机生成用户ID(0-2)
         * - 随机生成订单金额(0-100)
         * - 时间戳为当前系统时间
         */
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Order> orderDS = environment.addSource(new MyOrderSource()).setParallelism(2);

        orderDS.print();

        environment.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
    public static class MyOrderSource extends RichParallelSourceFunction<Order> {
        private Boolean flag = true;
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (flag){
                Thread.sleep(1000);
                String id = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(id,userId,money,createTime));
            }
        }
        //取消任务/执行cancle命令的时候执行
        @Override
        public void cancel() {
            flag = false;
        }
    }

}
