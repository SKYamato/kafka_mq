package co.zs.util;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka工具类
 *
 * @author shuai
 * @date 2020/03/19 9:16
 */
public class KafkaUtil {
    /**
     * 创建kafka客户端
     *
     * @return KafkaAdminClient
     */
    public static KafkaAdminClient createClient() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        return (KafkaAdminClient) KafkaAdminClient.create(props);
    }

    /**
     * 创建KafkaProducer
     *
     * @return KafkaProducer
     */
    public static KafkaProducer<String, String> createKafkaProducer() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化key-value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }

    /**
     * 分组创建KafkaConsumer
     *
     * @return KafkaConsumer
     */
    public static KafkaConsumer<String, String> createKafkaConsumerWithGroup(String groupId) {
        Properties props = getConsumerProperties();
        //配置消费者的组信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(props);
    }


    public static KafkaConsumer<String, String> createKafkaConsumerWithOutGroup() {
        Properties props = getConsumerProperties();
        return new KafkaConsumer<>(props);
    }

    /**
     * 获取KafkaConsumer配置信息
     *
     * @return properties
     */
    private static Properties getConsumerProperties() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //反序列化key-value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    /**
     * 关闭客户端
     *
     * @param client
     */
    public static void closeClient(KafkaAdminClient client) {
        client.close();
    }

    /**
     * 关闭生产者
     *
     * @param producer
     */
    public static void closeProducer(KafkaProducer<String, String> producer) {
        producer.close();
    }

    /**
     * 关闭消费者
     *
     * @param consumer
     */
    public static void closeConsumer(KafkaConsumer<String, String> consumer) {
        consumer.close();
    }
}
