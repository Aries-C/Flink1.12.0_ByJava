package connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Redisconn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 需求:
         * 接收消息并做WordCount,
         * 最后将结果保存到Redis
         * 注意:存储到Redis的数据结构:使用hash也就是map
         * key            value
         * WordCount    (单词,数量)
         */
        DataStream<String> lineDS = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(Tuple2.of(value, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);

        //连接单机版的redis
        FlinkJedisPoolConfig conf1 = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        //连接集群版Redis
        HashSet<InetSocketAddress> nodes = new HashSet<>(Arrays.asList(
                new InetSocketAddress(InetAddress.getByName("node1"), 6379),
                new InetSocketAddress(InetAddress.getByName("node2"), 6379),
                new InetSocketAddress(InetAddress.getByName("node3"), 6379)));
        FlinkJedisClusterConfig conf2 = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        //连接哨兵版Redis
        Set<String> sentinels = new HashSet<>(Arrays.asList("node1:26379", "node2:26379", "node3:26379"));
        FlinkJedisSentinelConfig conf3 = new FlinkJedisSentinelConfig.Builder().setMasterName("mymaster").setSentinels(sentinels).build();

        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf1, new RedisWordCountMapper()));

        env.execute();
    }
    /**
     * 定义一个 mapper 用来指定存储到 redis 的数据结构
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WordCount");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
