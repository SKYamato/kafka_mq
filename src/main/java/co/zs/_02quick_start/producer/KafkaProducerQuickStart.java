package co.zs._02quick_start.producer;

import co.zs.util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 生产消息
 *
 * @author shuai
 * @date 2020/03/19 9:15
 */
public class KafkaProducerQuickStart {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaUtil.createKafkaProducer();
        //生产者发送消息
        for (int i = 0; i < 10; i++) {
            sendRecord(producer, "topic01", "key" + i, "value" + i);
        }
        KafkaUtil.closeProducer(producer);
    }

    /**
     * kafka生产者发送消息
     *
     * @param producer
     * @param topicName
     * @param key
     * @param value
     */
    private static void sendRecord(KafkaProducer<String, String> producer, String topicName, String key, String value) {
        //创建消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        //发送消息
        producer.send(record);
    }
}
