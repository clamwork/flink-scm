package com.djcps.flink.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Chengw
 * @version 1.0.0
 * @since 2020/6/1 10:24.
 */
@Slf4j
public class SourceToMySql extends RichSourceFunction<PersonInfo> {

    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void run(SourceContext<PersonInfo> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            PersonInfo personInfo = new PersonInfo();
            personInfo.id =  resultSet.getInt(1);
            personInfo.name = resultSet.getString(2);
            personInfo.password = resultSet.getString(3);
            personInfo.age = resultSet.getInt(4);
            sourceContext.collect(personInfo);
        }
    }

    @Override
    public void cancel() {
        try {
            close();
        } catch (Exception e) {
            e.getStackTrace();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "select id,name,password,age from PersonInfo;";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
