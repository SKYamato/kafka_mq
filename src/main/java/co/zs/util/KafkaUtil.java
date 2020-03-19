package co.zs.util;

import co.zs._03patition.UserDefinePartitioner;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

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
     * @return kafkaAdminClient
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
     * @return kafkaProducer
     */
    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = getProducerProperties();
        return new KafkaProducer<>(props);
    }

    /**
     * 自定义partition
     *
     * @return
     */
    public static KafkaProducer<String, String> createKafkaProducerWithPartition() {
        Properties props = getProducerProperties();
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UserDefinePartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * 获取kafka生产者配置信息
     *
     * @return
     */
    private static Properties getProducerProperties() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //序列化key-value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * 创建带分组的KafkaConsumer
     *
     * @return kafkaConsumer
     */
    public static KafkaConsumer<String, String> createKafkaConsumerWithGroup(String groupId) {
        Properties props = getConsumerProperties();
        //配置消费者的组信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(props);
    }

    /**
     * 创建未分组的kafkaConsumer
     *
     * @return kafkaConsumer
     */
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
