package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSink;
import com.atguigu.app.func.MyDeserializationSchemaFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

//数据流：Web/App -> Nginx -> SpringBoot ->  Mysql -> Flink ->  Kafka(ods_base_db) -> Flink -> Kafka/Phoenix
//进程:           MockDB                 -> Mysql ->  FlinkCDCApp -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix
public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置应该与Kafka主题的分区数一致

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 读取Kafka ods_base_db 主题数据创建流
        String groupId = "base_db_app_group";
        String topic = "ods_base_db";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3 过滤空值数据(主流)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("发现脏数据。。。。" + value);
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj -> {
            String data = jsonObj.getString("data");
            return data != null && data.length() > 0;
        });

        //TODO 4 使用FlinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime-200923")
                .tableList("gmall-realtime-200923.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 5 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //TODO 6 分流
        OutputTag<JSONObject> hbaseOutPutTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> resultDS = connectedStream.process(new TableProcessFunction(hbaseOutPutTag, mapStateDescriptor));

        //TODO 7 将分好的流写入Phoenix表(维度数据)或者Kafka主题(事实数据)
        filterDS.print("主流原始数据>>>>>>>>");
        resultDS.print("Kafka>>>>>>>>");
        resultDS.getSideOutput(hbaseOutPutTag).print("HBase>>>>>>>>>>>");

        //将数据写入Phoenix
        resultDS.getSideOutput(hbaseOutPutTag).addSink(new DimSink());

        //将数据写入Kafka
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化Kafka数据");
            }

            //element:{"database":"","table":"","type":"","data":{"id":"11"...},"sinkTable":"dwd_xxx_xxx"}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        });
        resultDS.addSink(kafkaSinkBySchema);

        //TODO 8 启动任务
        env.execute();
    }

}
