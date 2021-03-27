package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TestKafkaConnect {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用Kafka连接器的方式创建动态表
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .property("bootstrap.servers", "hadoop102:9092")
                .property("group.id", "testGroup")
                .startFromLatest())
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING()))
                .createTemporaryTable("user_info");

        tableEnv.executeSql("select id,count(*) from user_info group by id").print();
    }

}
