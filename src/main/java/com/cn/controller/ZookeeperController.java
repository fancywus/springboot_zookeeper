package com.cn.controller;

import com.cn.pojo.Node;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/zookeeper")
public class ZookeeperController {

    private final Stat stat = new Stat();

    private List<String> childrenNodeList = new ArrayList<>();

    private Node nodeMsg = new Node();

    @Autowired
    private CuratorFramework curatorFramework;

    private List<String> getTopNode(String nodeStr) throws Exception {
        return curatorFramework.getChildren().forPath(nodeStr);
    }

    @GetMapping("create/temp/{node}")
    public Object createTempNode(@PathVariable String node) throws Exception {
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath("/" + node);
        childrenNodeList = getTopNode("/");
        return childrenNodeList;
    }

    @GetMapping("create/{node}")
    public Object createNode(@PathVariable String node) throws Exception {
        curatorFramework.create().creatingParentsIfNeeded().forPath("/" + node);
        childrenNodeList = getTopNode("/");
        return childrenNodeList;
    }

    @GetMapping("get/{node}")
    public Object queryNode(@PathVariable String node) throws Exception {
        byte[] bytes = curatorFramework.getData().forPath("/" + node);
        curatorFramework.getData().storingStatIn(stat).forPath("/" + node);
        childrenNodeList = getTopNode("/" + node);
        nodeMsg.setStat(stat);
        nodeMsg.setData(new String(bytes));
        nodeMsg.setChildrenList(childrenNodeList);
        return nodeMsg;
    }

    @PutMapping("set/{node}/{data}")
    public Object setNodeData(@PathVariable String node,
                              @PathVariable String data) throws Exception {
        String setNode = "/" + node;
        curatorFramework.getData().storingStatIn(stat).forPath(setNode);
        Integer version = stat.getVersion();
        curatorFramework.setData().withVersion(version).forPath(setNode, data.getBytes());
        childrenNodeList = getTopNode(setNode);
        nodeMsg.setStat(stat);
        nodeMsg.setData(data);
        nodeMsg.setChildrenList(childrenNodeList);
        return nodeMsg;
    }

    @DeleteMapping("del/{node}")
    public Object delNode(@PathVariable String node) throws Exception {
        String setNode = "/" + node;
        curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(setNode);
        childrenNodeList = getTopNode("/");
        return childrenNodeList;
    }
}
