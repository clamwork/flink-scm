package com.djcps.flink;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author cw
 * @date 2020/5/31
 * @time 13:28
 * @since 1.0.0
 **/
public class SqlWriteFunction<T> implements RichFunction{

    Connection conn = null;
    PreparedStatement ps = null;

    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://node01:3306/test";

    String username = "root";
    String password = "root";

    @Override
    public void open(Configuration configuration) throws Exception {
        Class.forName(driver);
        conn = DriverManager.getConnection(url, username, password);
        conn.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        if(conn != null){
            conn.close();
        }
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return null;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {

    }
}
