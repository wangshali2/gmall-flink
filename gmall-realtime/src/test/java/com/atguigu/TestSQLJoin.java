package com.atguigu;

import com.atguigu.bean.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class TestSQLJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<User> user1 = env.socketTextStream("hadoop102", 8888)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new User(fields[0], fields[1]);
                });
        SingleOutputStreamOperator<User> user2 = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new User(fields[0], fields[1]);
                });

        //将2个流注册为表
        Table userTable1 = tableEnv.fromDataStream(user1);
        tableEnv.createTemporaryView("u1", userTable1);
        Table userTable2 = tableEnv.fromDataStream(user2);
        tableEnv.createTemporaryView("u2", userTable2);

        //定义状态超时时间
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //JOIN:    两表状态数据为   OnCreateAndWrite  方式保存
        //LeftJOIN:左表状态数据为   OnReadAndWrite    右表为   OnCreateAndWrite
        //RightJOIN:左表状态数据为  OnCreateAndWrite  右表为   OnReadAndWrite
        //FullJOIN:两表状态数据为   OnReadAndWrite    方式保存
        tableEnv.executeSql("select * from u1 full join u2 on u1.id=u2.id")
                .print();

    }

}
