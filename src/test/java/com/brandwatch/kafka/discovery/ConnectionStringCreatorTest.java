package com.brandwatch.kafka.discovery;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ConnectionStringCreatorTest {

    private ConnectionStringCreator connectionStringCreator;

    @Before
    public void setup() {
        connectionStringCreator = new ConnectionStringCreator();
    }

    @Test(expected = NullPointerException.class)
    public void whenNullIsPassed_createConnectionString_throwsANullPointerException() {
        connectionStringCreator.createConnectionString(null);
    }

    @Test
    public void whenNoBrokersAreGiven_createConnectionString_returnsEmptyString() {
        connectionStringCreator.createConnectionString(new ArrayList<BrokerInfo>());
    }

    @Test
    public void whenOneBrokerIsGiven_createConnectionString_returnsThatBrokerAlone() {
        List<BrokerInfo> brokers = new ArrayList<BrokerInfo>();
        BrokerInfo brokerInfo = new BrokerInfo();
        brokerInfo.setHost("host1");
        brokerInfo.setPort(5555);
        brokers.add(brokerInfo);
        String connectionString = connectionStringCreator.createConnectionString(brokers);
        Assert.assertEquals("host1:5555", connectionString);
    }

    @Test
    public void whenMultipleBrokersAreGiven_createConnectionString_returnsACommaSeparatedString() {
        List<BrokerInfo> brokers = new ArrayList<BrokerInfo>();
        BrokerInfo brokerInfo1 = new BrokerInfo();
        brokerInfo1.setHost("host1");
        brokerInfo1.setPort(5555);
        brokers.add(brokerInfo1);
        BrokerInfo brokerInfo2 = new BrokerInfo();
        brokerInfo2.setHost("host2");
        brokerInfo2.setPort(4444);
        brokers.add(brokerInfo2);
        BrokerInfo brokerInfo3 = new BrokerInfo();
        brokerInfo3.setHost("host3");
        brokerInfo3.setPort(3333);
        brokers.add(brokerInfo3);
        String connectionString = connectionStringCreator.createConnectionString(brokers);
        Assert.assertEquals("host1:5555,host2:4444,host3:3333", connectionString);
    }
}
