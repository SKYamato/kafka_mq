package co.zs._06offsets.v2;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * consumer初始offset策略为earliest
 *
 * @author shuai
 * @date 2020/03/19 15:07
 */
public class KafkaConsumerOffsetsEarliest {
    public static void main(String[] args) {
        Properties pros = KafkaUtil.getConsumerBaseProperties();
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "a2p");
        /**
         * 没有记录当前消费者offset值时候
         * offset默认配置为latest
         *    latest:     读取最新的offset
         *    earliest:   读取最早的偏移量
         *    none:       报错
         */
        pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        //订阅相关的topics
        consumer.subscribe(Arrays.asList("topic01"));
        //处理结果
        KafkaUtil.getConsumerResult(consumer);
    }
}
