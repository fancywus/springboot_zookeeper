package com.cn.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
@Slf4j
public class curatorTest {

    private CuratorFramework client;

    /**
     * 和zookeeper服务器建立连接
     */
    @BeforeEach
    public void testConnect() {
        // 1.第一种方式
        /*
         * 指定一种重试策略，当前重试策略表示间隔时间内重试，最大可以重试几次
         * @param baseSleepTimeMs initial amount of time to wait between retries 重试一次休眠多久,单位ms
         * @param maxRetries max number of times to retry 最大重试次数
         */
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(3000, 10);
        /*
         * @param connectString       list of servers to connect to zk server 连接地址+端口号，多个集群地址用逗号间隔
         * @param sessionTimeoutMs    session timeout 会话超时时间 单位ms
         * @param connectionTimeoutMs connection timeout 连接超时时间 单位ms
         * @param retryPolicy         retry policy to use 重试策略，有多个重试策略的实现
         */
        // CuratorFramework client = CuratorFrameworkFactory
        //         .newClient("121.37.238.132:2181", 60 * 1000, 15 * 1000, exponentialBackoffRetry);
        // // 手动开启连接
        // client.start();
        // 2.第二种方式 .namespace("demo").表示命名空间，这个链接下的创建节点都会默认到该命名空间下 /demo的路径
        // CuratorFramework client = CuratorFrameworkFactory.builder().connectString("121.37.238.132:2181")
        //         .sessionTimeoutMs(60 * 1000).connectionTimeoutMs(15 * 1000)
        //         .retryPolicy(exponentialBackoffRetry).namespace("demo").build();
        client = CuratorFrameworkFactory.builder().connectString("121.37.238.132:2181")
                .sessionTimeoutMs(60 * 1000).connectionTimeoutMs(15 * 1000)
                .retryPolicy(exponentialBackoffRetry).namespace("demo").build();
        client.start();
    }

    /**
     * 创建持久化、临时节点、多级节点方法
     * @throws Exception
     */
    @Test
    public void createNode() throws Exception {
        // 1.基本创建
        String path1 = client.create().forPath("/app1");
        log.info("{} 节点已创建*****", path1);
        String path2 = client.create().forPath("/app2", "hello,node: /app2".getBytes());
        log.info("{} 节点已创建*****", path2);
        String path3 = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        log.info("{} 节点已创建*****", path3);
        String path4 = client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/app4", "hello,node: /app4".getBytes());
        log.info("{} 节点已创建*****", path4);
        // creatingParentsIfNeeded()表示如果父级节点不存在则创建，不加此方法直接创建多级节点会报错
        String path5 = client.create().creatingParentsIfNeeded().forPath("/app5/p1", "hello,node: /app5/p1".getBytes());
        log.info("{} 节点已创建*****", path5);
    }

    /**
     * 查询节点、子节点数据好状态信息方法
     * @throws Exception
     */
    @Test
    public void queryNode() throws Exception {
        Stat stat = new Stat();
        byte[] bytes1 = client.getData().forPath("/app1");
        client.getData().storingStatIn(stat).forPath("/app1");
        log.info("/app1的数据是 {}", new String(bytes1));
        log.info("/app1的节点状态是 {}", stat);
        log.info("**************************************************");
        byte[] bytes2 = client.getData().forPath("/app2");
        client.getData().storingStatIn(stat).forPath("/app2");
        log.info("/app2的数据是 {}", new String(bytes2));
        log.info("/app2的节点状态是 {}", stat);
        log.info("**************************************************");
        byte[] bytes3 = client.getData().forPath("/app40000000003");
        client.getData().storingStatIn(stat).forPath("/app40000000003");
        log.info("/app40000000003的数据是 {}", new String(bytes3));
        log.info("/app40000000003的节点状态是 {}", stat);
        log.info("**************************************************");
        byte[] bytes4 = client.getData().forPath("/app5");
        client.getData().storingStatIn(stat).forPath("/app5");
        log.info("/app5的数据是 {}", new String(bytes4));
        log.info("/app5的节点状态是 {}", stat);
        log.info("**************************************************");
        List<String> list1 = client.getChildren().forPath("/app5");
        client.getData().storingStatIn(stat).forPath("/app5/p1");
        log.info("/app5的子节点数据是 {}", list1.toString());
        log.info("/app5/p1的节点状态是 {}", stat);
        log.info("**************************************************");
        List<String> list = client.getChildren().forPath("/");
        client.getData().storingStatIn(stat).forPath("/");
        log.info("/demo的子节点数据是 {}", list.toString());
        log.info("/demo的节点状态是 {}", stat);
    }

    /**
     * 修改节点数据方法
     * @throws Exception
     */
    @Test
    public void setNodeData() throws Exception {
        client.setData().forPath("/app1", "hello, Node: /app1".getBytes());
        byte[] bytes = client.getData().forPath("/app1");
        log.info("/app1的节点数据为 {}", new String(bytes));
    }

    /**
     * 根据版本号修改节点数据，用乐观锁的版本号维护数据一致性
     * @throws Exception
     */
    @Test
    public void setNodeDataByVersion() throws Exception {
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app5");
        // 获取当前数据的最新版本号
        Integer version = stat.getVersion();
        log.info("当前版本号：{}", version);
        // 如果当前版本号和zookeeper最新版本号不一致就会报错，乐观锁
        client.setData().withVersion(version).forPath("/app5", "this is node: /app5".getBytes());
        byte[] bytes = client.getData().forPath("/app5");
        log.info("/app5的节点数据为 {}", new String(bytes));
    }

    @Test
    public void queryTopNode() throws Exception {
        List<String> list = client.getChildren().forPath("/");
        log.info("/demo的子节点数据是 {}", list.toString());
    }

    /**
     * 多种删除节点方法，包括重试机制删除和删除后的回调方法
     * @throws Exception
     */
    @Test
    public void delNode() throws Exception {
        queryTopNode();
        client.delete().forPath("/app1");
        queryTopNode();
        // 删除带有子节点的节点
        client.delete().deletingChildrenIfNeeded().forPath("/app5");
        queryTopNode();
        // 带有重试机制的删除，保证删除指令可以发送过去
        client.delete().guaranteed().forPath("/app2");
        queryTopNode();
        // 带有回调机制的删除方法，返回client客户端对象和一个event对象
        client.delete().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                log.info("回调通知: 删除完成");
                log.info("event: {}", event.toString());
            }
        }).forPath("/app40000000003");
        queryTopNode();
    }

    @AfterEach
    public void closeConnection() {
        if (client != null) {
            client.close();
        }
    }
}
