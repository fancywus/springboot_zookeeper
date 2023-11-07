package com.cn.conf;

import com.cn.pojo.ZKProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@SpringBootConfiguration
public class ZookeeperConnection {

    @Autowired
    private ZKProperties zkProperties;

    @Bean
    public CuratorFramework curatorFramework() {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(zkProperties.getRetry().getBaseSleepTimeMs(),
                zkProperties.getRetry().getBaseSleepTimeMs());
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkProperties.getConnectString())
                .sessionTimeoutMs(zkProperties.getSessionTimeOut())
                .connectionTimeoutMs(zkProperties.getConnectionTimeOut())
                .retryPolicy(retry).namespace(zkProperties.getNameSpace()).build();
        client.start();
        return client;
    }
}
