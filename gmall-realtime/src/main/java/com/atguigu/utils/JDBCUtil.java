package com.atguigu.utils;

import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    /**
     * @param sql               查询语句
     * @param underScoreToCamel 是否需要将下划线命名转换为驼峰命名
     * @param <T>               返回值类型
     * @return 集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {


        //创建集合用于存放结果数据
        ArrayList<T> result = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall-realtime-200923?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "000000"
            );

            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //执行查询
            resultSet = preparedStatement.executeQuery();

            //获取查询结果中的列信息
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                //通过反射的方式获取对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    //获取值信息
                    String value = resultSet.getString(i);

                    //给对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                }

                //将对象放入集合
                result.add(t);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        //返回结果集
        return result;
    }


    public static void main(String[] args) throws Exception {

        List<TableProcess> tableProcesses = queryList(
                "select * from table_process",
                TableProcess.class,
                true);

        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
            System.out.println("\n");
        }

    }

}
