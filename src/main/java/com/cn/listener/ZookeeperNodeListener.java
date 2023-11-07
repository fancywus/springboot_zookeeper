package com.cn.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * watch监听节点变更
 */
// @Component
@Slf4j
public class ZookeeperNodeListener {

    @Autowired
    private CuratorFramework curatorFramework;

    /*
     * nodeCache可以指定监听某个节点变更信息
     */
    @Bean
    public void nodeCache() throws Exception {
        // 1.创建NodeCache对象
        final NodeCache nodeCache = new NodeCache(curatorFramework, "/app1");
        // 2.注册监听和编写监听逻辑
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() {
                if (nodeCache.getCurrentData() != null) {
                    log.info("当前节点数据变更信息: {}", new String(nodeCache.getCurrentData().getData()));
                    log.info("当前节点数据状态信息: {}", nodeCache.getCurrentData().getStat());
                }
                else {
                    log.info("当前节点数据状态被删除！");
                }
            }
        });
        // 3.开启监听
        nodeCache.start(true);
    }

    /*
     * 可以指定监听某个节点的子节点变更信息
     * @param client           the client zookeeper客户端对象
     * @param path             path to watch 需要传入的节点名称，便会监听节点所有子节点变更信息
     * @param cacheData        if true, node contents are cached in addition to the stat 为true，则会缓存节点状态信息
     * @param dataIsCompressed if true, data in the path is compressed 是否压缩数据
     * @param executorService  Closeable ExecutorService to use for the PathChildrenCache's background thread.  线程池
     * This service should be single threaded, otherwise the cache may see inconsistent results.
     */
    @Bean
    public void pathChildrenCache() throws Exception {
        // 1.创建pathChildrenCache对象
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, "/app3", true);
        // 2.注册监听和编写监听逻辑
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (PathChildrenCacheEvent.Type.CHILD_ADDED.equals(event.getType()) || PathChildrenCacheEvent.Type.CHILD_UPDATED.equals(event.getType())) {
                    log.info("当前子节点数据信息: {}", event.getData().getData() != null ? new String(event.getData().getData()) : null);
                    log.info("当前子节点数据状态信息: {}", event.getData().getStat());
                }
                else if (PathChildrenCacheEvent.Type.CHILD_REMOVED.equals(event.getType())) {
                    log.info("当前子节点数据变更信息: {}", event);
                }
            }
        });
        // 3.开启监听
        pathChildrenCache.start();
    }

    /*
     * 可以监听指定节点及其子节点的变更信息
     */
    @Bean
    public void treeCache() throws Exception {
        // 1.创建treeCache对象
        TreeCache treeCache = new TreeCache(curatorFramework, "/app2");
        // 2.注册监听和编写监听逻辑
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) {
                log.info("当前父或其子节点数据变更信息: {}", event);
            }
        });
        // 3.开启监听
        treeCache.start();
    }
}
