package com.brandwatch.kafka.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperConnector {

    private CuratorFramework curatorFramework;

    public ZookeeperConnector(String hostAndPort) {
        curatorFramework = CuratorFrameworkFactory
            .builder()
            .connectString(hostAndPort)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();
        curatorFramework.start();
    }

    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }
}
