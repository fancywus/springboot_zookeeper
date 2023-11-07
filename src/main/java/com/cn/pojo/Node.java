package com.cn.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.zookeeper.data.Stat;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Node {

    private Stat stat;

    private String data;

    private List<String> childrenList;
}
