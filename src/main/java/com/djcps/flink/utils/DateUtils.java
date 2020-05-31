package com.djcps.flink.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author cw
 * @date 2020/5/31
 * @time 0:19
 * @since 1.0.0
 **/
public class DateUtils {
    private DateUtils(){}

    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    public static String now(){
        LocalDateTime localDateTime = LocalDateTime.now();
        return localDateTime.format(DEFAULT_DATE_FORMATTER);
    }
}
