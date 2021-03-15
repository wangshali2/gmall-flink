package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2.使用FlinkSQL  DDL方式读取MySQL变化数据
        tableEnv.executeSql("CREATE TABLE user_info ( " +
                " id INT NOT NULL, " +
                " name STRING, " +
                " phone_num STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'hadoop102', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '000000', " +
                " 'database-name' = 'gmall-flink-200821', " +
                " 'table-name' = 'z_user_info' " +
                ")");

        //3.打印
        Table table = tableEnv.sqlQuery("select * from user_info");
        tableEnv.toRetractStream(table, Row.class).print();

        //4.开启任务
        env.execute();

    }

}
