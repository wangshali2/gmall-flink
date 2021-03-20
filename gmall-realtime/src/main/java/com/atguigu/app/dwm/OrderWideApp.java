package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

//数据流: Web/App -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd/dim) ->
//       FlinkApp -> Kafka(dwm)
//进程:           MockDB                 ->Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDBApp -> Kafka(Phoenix)          -> OrderWideApp -> Kafka
public class OrderWideApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2 读取Kafka dwd_order_info dwd_order_detail主题的数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_app_group";
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(groupId, orderInfoSourceTopic);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(groupId, orderDetailSourceTopic);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        orderInfoJsonStrDS.print("原始OrderInfo>>>>>>>>>>");
        orderDetailJsonStrDS.print("原始OrderDetail>>>>>>>>>>");

        //TODO 3 将2个流转换为JavaBean并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(data -> {

            OrderInfo orderInfo = JSON.parseObject(data, OrderInfo.class);

            //补充其中的时间字段
            String create_time = orderInfo.getCreate_time(); //yyyy-MM-dd HH:mm:ss

            System.out.println(create_time);

            String[] dateHourArr = create_time.split(" ");
            orderInfo.setCreate_date(dateHourArr[0]);
            orderInfo.setCreate_hour(dateHourArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(data -> {

            OrderDetail orderDetail = JSON.parseObject(data, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());

            return orderDetail;

        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4 双流JOIN
        KeyedStream.IntervalJoined<OrderInfo, OrderDetail, Long> joinedDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .inEventTime()
                .between(Time.seconds(-5), Time.seconds(5));
        SingleOutputStreamOperator<OrderWide> orderWideDS = joinedDS.process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                collector.collect(new OrderWide(orderInfo, orderDetail));
            }
        });

        orderWideDS.print("OrderWide>>>>>>>>>>");

        //TODO 5 查询Phoenix,补全维度信息
        //5.1 关联用户维度
        orderWideDS.map(orderWide -> {
            Long user_id = orderWide.getUser_id();
            JSONObject userDim = DimUtil.getDim("", user_id.toString());
            return orderWide;
        });
        //5.2 关联省份维度
        //5.3 关联SKU维度
        //5.4 关联SPU维度
        //5.5 关联品牌维度
        //5.6 关联品类维度

        //TODO 6 将数据写入Kafka

        //TODO 7 启动任务'
        env.execute();

    }

}
