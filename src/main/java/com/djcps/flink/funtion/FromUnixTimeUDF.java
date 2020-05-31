package com.djcps.flink.funtion;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Administrator
 */
public class FromUnixTimeUDF extends ScalarFunction {
    public String DATE_FORMAT;

    public FromUnixTimeUDF() {
        this.DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    }

    public FromUnixTimeUDF(String dateFormat) {
        this.DATE_FORMAT = dateFormat;
    }

    public String eval(String longTime) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            Date date = new Date(Long.parseLong(longTime) * 1000);
            return sdf.format(date);
        } catch (Exception e) {
            return null;
        }
    }

    public String eval(String longTime, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Date date = new Date(Long.parseLong(longTime) * 1000);
            return sdf.format(date);
        } catch (Exception e) {
            return null;
        }
    }
}