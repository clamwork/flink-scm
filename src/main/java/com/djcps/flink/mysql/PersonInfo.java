package com.djcps.flink.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by cw on 2019-02-17
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonInfo {
    public int id;
    public String name;
    public String password;
    public int age;
}
