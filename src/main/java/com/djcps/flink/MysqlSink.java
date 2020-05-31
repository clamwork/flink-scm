package com.djcps.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @author cw
 * @date 2020/5/31
 * @time 16:17
 * @since 1.0.0
 **/
public class MysqlSink extends RichSinkFunction<Tuple3<Integer, String, Integer>> {

    private Connection connection;
    private PreparedStatement preparedStatement;
    String userName = "root";
    String password = "root";
    String driverName = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/test";

    @Override
    public void invoke(Tuple3<Integer, String, Integer> value, Context context) throws Exception {
        Class.forName(driverName);
        connection = DriverManager.getConnection(url, userName, password);
        String sql = "replace into table(id,num,price) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setInt(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
