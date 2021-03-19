package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    //准备配置信息
    private static Properties properties = new Properties();

    //指定DWD事实数据默认主题
    private static final String DWD_DEFAULT_TOPIC = "dwd_default_topic";

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
    }

    /**
     * 获取Kafka生产者
     *
     * @param topic 写入Kafka的主题名
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSchema) {

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<T>(DWD_DEFAULT_TOPIC,
                kafkaSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取Kafka的消费者
     *
     * @param groupId 消费者组
     * @param topic   消费的主题
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

}
