package com.djcps.flink.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Desc:
 * Created by cw on 2019-02-17
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonInfo {
    public Integer id;
    public String name;
    public String password;
    public Integer age;
}
