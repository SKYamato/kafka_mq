package co.zs.dml;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 测试Kafka DML
 *
 * @author shuai
 * @date 2020/03/18 17:17
 */
public class KafkaTopicDML {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        //创建KafkaAdminClient
        KafkaAdminClient client = (KafkaAdminClient) KafkaAdminClient.create(props);

        //异步创建Topic信息
        //client.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 1)));

        //查看topic列表
        ListTopicsResult topicsResult = client.listTopics();
        Set<String> names = topicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }
        //关闭客户端
        client.close();
    }
}
