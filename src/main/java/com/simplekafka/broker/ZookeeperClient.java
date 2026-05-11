package com.simplekafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;
    
    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    
    public ZookeeperClient (String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();
        
        // Create required paths if not exist
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }
    
    public String getConnectString() {
        return host + ":" + port;
    }
    
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
}
