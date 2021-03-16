package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {


    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        //准备配置信息
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        //创建Kafka生产者对象并返回
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);

    }

}
