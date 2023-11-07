package com.cn.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

@Slf4j
public class TicketSale implements Runnable {

    private volatile int ticketCount = 10;

    private InterProcessMutex interProcessMutex;

    public TicketSale() {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(3000, 10);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("121.37.238.132:2181")
                .sessionTimeoutMs(60 * 1000).connectionTimeoutMs(15 * 1000)
                .retryPolicy(retry).build();
        client.start();
        this.interProcessMutex = new InterProcessMutex(client, "/lock");
    }

    /**
     * 实现多线程的run方法，里面实现买票业务
     */
    @Override
    public void run() {
        while (ticketCount > 0) {
            try {
                interProcessMutex.acquire(3, TimeUnit.SECONDS);
                if (ticketCount > 0) {
                    // 获得锁
                    log.info("当前软件 {}, 为您抢到第{}张票", Thread.currentThread(), ticketCount);
                    ticketCount --;
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                try {
                    // 释放锁
                    interProcessMutex.release();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
