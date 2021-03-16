package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializationSchemaFunction;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.2 CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.使用DataStream方式的CDC读取MySQL变化数据(只读取最新的数据)
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink-200923")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        //3.将数据写入Kafka
        String topic = "ods_base_db";
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink(topic));
        mysqlDS.print(">>>>>>>");

        //4.启动任务
        env.execute();

    }

}
