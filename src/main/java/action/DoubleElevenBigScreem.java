package action;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 实时计算出当天零点截止到当前时间的销售总额
 * 计算出各个分类的销售top3
 * 每秒钟更新一次统计结果
 */
public class DoubleElevenBigScreem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Double>> sourceDS = env.addSource(new MySource());

        SingleOutputStreamOperator<CategoryPojo> tempAggResult = sourceDS
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new PriceAggregate(), new WindowResult());

        tempAggResult.print("初步聚合结果：");

        tempAggResult
                //按照时间分组是因为需要每1s更新截至到当前时间的销售总额
                .keyBy("dateTime")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new WindowResultProcess());

        env.execute();
    }

    /**
     * 自定义数据源实时产生订单数据Tuple2<分类, 金额>
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private boolean flag = true;
        private String[] categorys = {"女装", "男装","图书", "家电","洗护", "美妆","运动", "游戏","户外", "家具","乐器", "办公"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag){
                //随机生成分类和金额
                int index = random.nextInt(categorys.length);//[0~length) ==> [0~length-1]
                String category = categorys[index];//获取的随机分类
                double price = random.nextDouble() * 100;//注意nextDouble生成的是[0~1)之间的随机数,*100之后表示[0~100)
                ctx.collect(Tuple2.of(category,price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        //初始化累加器为0
        @Override
        public Double createAccumulator() {
            return 0D; //D表示Double,L表示long
        }

        //把price往累加器上累加
        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        //获取累加结果
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        //各个subTask的结果合并
        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;//分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;// 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
    }

    /**
     * 自定义WindowFunction,实现如何收集窗口结果数据
     * interface WindowFunction<IN, OUT, KEY, W extends Window>
     * interface WindowFunction<Double, CategoryPojo, Tuple的真实类型就是String就是分类, W extends Window>
     */
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        //定义一个时间格式化工具用来将当前时间(双十一那天订单的时间)转为String格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            String category = ((Tuple1<String>) tuple).f0;

            Double price = input.iterator().next();
            //为了后面项目铺垫,使用一下用Bigdecimal来表示精确的小数
            BigDecimal bigDecimal = new BigDecimal(price);
            //setScale设置精度保留2位小数,
            double roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//四舍五入

            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);

            CategoryPojo categoryPojo = new CategoryPojo(category, roundPrice, dateTime);
            out.collect(categoryPojo);
        }
    }

    /**
     * 实现ProcessWindowFunction
     * abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
     * abstract class ProcessWindowFunction<CategoryPojo, Object, Tuple就是String类型的dateTime, TimeWindow extends Window>
     *
     * 把各个分类的总价加起来，就是全站的总销量金额，
     * 然后我们同时使用优先级队列计算出分类销售的Top3，
     * 最后打印出结果，在实际中我们可以把这个结果数据存储到hbase或者redis中，以供前端的实时页面展示。
     */
    private static class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            String dateTime = ((Tuple1<String>)tuple).f0;
            //Java中的大小顶堆可以使用优先级队列来实现
            //https://blog.csdn.net/hefenglian/article/details/81807527
            //注意:
            // 小顶堆用来计算:最大的topN
            // 大顶堆用来计算:最小的topN
            Queue<CategoryPojo> queue = new PriorityQueue<>(3,//初识容量
                    //正常的排序,就是小的在前,大的在后,也就是c1>c2的时候返回1,也就是小顶堆
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);

            //在这里我们要完成需求:
            // * -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额,其实就是把之前的初步聚合的price再累加!
            double totalPrice = 0D;
            double roundPrice = 0D;
            Iterator<CategoryPojo> iterator = elements.iterator();
            for (CategoryPojo element : elements) {
                double price = element.totalPrice;//某个分类的总销售额
                totalPrice += price;
                BigDecimal bigDecimal = new BigDecimal(totalPrice);
                roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//四舍五入
                // * -2.计算出各个分类的销售额top3,其实就是对各个分类的price进行排序取前3
                //注意:我们只需要top3,也就是只关注最大的前3个的顺序,剩下不管!所以不要使用全局排序,只需要做最大的前3的局部排序即可
                //那么可以使用小顶堆,把小的放顶上
                // c:80
                // b:90
                // a:100
                //那么来了一个数,和最顶上的比,如d,
                //if(d>顶上),把顶上的去掉,把d放上去,再和b,a比较并排序,保证顶上是最小的
                //if(d<=顶上),不用变
                if (queue.size() < 3) {//小顶堆size<3,说明数不够,直接放入
                    queue.add(element);
                }else{//小顶堆size=3,说明,小顶堆满了,进来一个需要比较
                    //"取出"顶上的(不是移除)
                    CategoryPojo top = queue.peek();
                    if(element.totalPrice > top.totalPrice){
                        //queue.remove(top);//移除指定的元素
                        queue.poll();//移除顶上的元素
                        queue.add(element);
                    }
                }
            }
            // * -3.每1秒钟更新一次统计结果,可以直接打印/sink,也可以收集完结果返回后再打印,
            //   但是我们这里一次性处理了需求1和2的两种结果,不好返回,所以直接输出!
            //对queue中的数据逆序
            //各个分类的销售额top3
            List<String> top3Result = queue.stream()
                    .sorted((c1, c2) -> c1.getTotalPrice() > c2.getTotalPrice() ? -1 : 1)//逆序
                    .map(c -> "(分类：" + c.getCategory() + " 销售总额：" + c.getTotalPrice() + ")")
                    .collect(Collectors.toList());
            System.out.println("时间 ： " + dateTime + "  总价 : " + roundPrice + " top3:\n" + StringUtils.join(top3Result, ",\n"));
            System.out.println("-------------");

        }
    }

}
