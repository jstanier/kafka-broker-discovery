package com.brandwatch.kafka.discovery;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaBrokerDiscovererTest {

    @Mock
    private ConnectionStringCreator connectionStringCreator;

    @Mock
    private BrokerInfoFetcher brokerInfoFetcher;

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void whenGivenANullHost_getConnectionString_throwsANullPointerException()
            throws Exception {
        new KafkaBrokerDiscoverer(null, "5555");
    }

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void whenGivenANullPort_getConnectionString_throwsANullPointerException()
            throws Exception {
        new KafkaBrokerDiscoverer("host1", null);
    }
}
