package co.zs.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
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
        //创建KafkaAdminClient
        KafkaAdminClient client = getClient();

        //创建topic
        //createTopic(client);

        //删除topic
        //deleteTopics(client, "topic02");

        //查看topic详情
        searchDetail(client);

        //查看topic列表
        searchTopic(client);

        //关闭客户端
        client.close();
    }

    /**
     * 创建kafka客户端
     *
     * @return
     */
    private static KafkaAdminClient getClient() {
        //设置链接参数
        Properties props = new Properties();
        //需要在host文件中做ip和主机名的映射
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092");
        return (KafkaAdminClient) KafkaAdminClient.create(props);
    }

    /**
     * 创建topic
     *
     * @param client
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static void createTopic(KafkaAdminClient client) throws InterruptedException, ExecutionException {
        //异步创建Topic信息
        CreateTopicsResult topics = client.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 1),
                new NewTopic("topic02", 3, (short) 1)));

        //同步创建Topic信息
        topics.all().get();
    }

    /**
     * 删除topic
     *
     * @param client
     * @param topicNames
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void deleteTopics(KafkaAdminClient client, String... topicNames) throws ExecutionException, InterruptedException {
        //异步删除
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(topicNames));

        //同步删除
        deleteTopicsResult.all().get();
    }

    private static void searchDetail(KafkaAdminClient client) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : stringTopicDescriptionMap.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
    }

    /**
     * 查询所有的topic信息
     *
     * @param client
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static void searchTopic(KafkaAdminClient client) throws InterruptedException, ExecutionException {
        ListTopicsResult topicsResult = client.listTopics();
        Set<String> names = topicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }
    }
}
