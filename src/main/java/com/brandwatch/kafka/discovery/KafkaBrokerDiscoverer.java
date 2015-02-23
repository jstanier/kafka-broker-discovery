package com.brandwatch.kafka.discovery;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class KafkaBrokerDiscoverer implements Closeable {

    private ConnectionStringCreator connectionStringCreator;
    private BrokerInfoFetcher dataFetcher;
    private List<BrokerInfo> brokers;

    public KafkaBrokerDiscoverer(String host, String port) throws Exception {
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(port);
        connectionStringCreator = new ConnectionStringCreator();
        String zookeeperConnectionString = Joiner.on(":").join(host, port);
        dataFetcher = new BrokerInfoFetcher(zookeeperConnectionString);
        brokers = dataFetcher.fetchBrokerInfo();
    }

    public String getConnectionString() throws Exception {
        return connectionStringCreator.createConnectionString(brokers);
    }

    public void close() throws IOException {
        dataFetcher.close();
    }

}
