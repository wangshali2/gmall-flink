package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class FlinkCDCWithMySchema {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取MySQL变化数据
        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink-200821","gmall-realtime-200821")
//                .tableList("gmall-flink-200821.z_user_info","gmall-flink-200821.base_category1") //注意：传入参数时需要"db.table"方式
                .deserializer(new MyDeserializationSchema())
                .startupOptions(StartupOptions.initial())    //指定启动模式
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.打印数据
        streamSource.print();

        //4.启动任务
        env.execute();

    }

    public static class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //定义JSON对象用于存放反序列化后的数据
            JSONObject result = new JSONObject();

            //获取库名和表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];
            String table = split[2];

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取数据本身
            Struct struct = (Struct) sourceRecord.value();
            Struct after = struct.getStruct("after");
            JSONObject value = new JSONObject();

            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    value.put(field.name(), after.get(field.name()));
                }
            }

            //将数据放入JSON对象
            result.put("database", database);
            result.put("table", table);
            result.put("operation", operation.toString().toLowerCase());
            result.put("value", value);

            //将数据传输出去
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

}
