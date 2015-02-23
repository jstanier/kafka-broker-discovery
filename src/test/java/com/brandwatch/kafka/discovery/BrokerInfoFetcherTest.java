package com.brandwatch.kafka.discovery;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class BrokerInfoFetcherTest {

    @Mock
    private ZookeeperConnector zookeeperConnector;

    @Mock
    private CuratorFramework curatorFramework;

    @Mock
    private DataParser dataParser;

    @Mock
    private PathChildrenCache pathChildrenCache;

    @Mock
    private ExistsBuilder existsBuilder;

    private BrokerInfoFetcher brokerInfoFetcher;

    @Before
    public void setup() throws Exception {
        brokerInfoFetcher = Mockito.spy(new BrokerInfoFetcher("localhost:2181"));

        Whitebox.setInternalState(brokerInfoFetcher, "zookeeperConnector", zookeeperConnector);
        Mockito.when(zookeeperConnector.getCuratorFramework()).thenReturn(curatorFramework);
        Mockito.doReturn(pathChildrenCache).when(brokerInfoFetcher).makePathChildrenCache();
        Mockito
            .doReturn(zookeeperConnector)
            .when(brokerInfoFetcher)
            .makeZookeeperConnector(Mockito.anyString());
        Mockito.doReturn(dataParser).when(brokerInfoFetcher).makeDataParser();
        Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    }

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void givenANullArgument_constructor_throwsANullPointerException() throws Exception {
        new BrokerInfoFetcher(null);
    }

    @SuppressWarnings("resource")
    @Test
    public void givenAValidHostAndPort_constructor_succeeds() throws Exception {
        new BrokerInfoFetcher("localhost:2181");
    }

    @Test
    public void whenItIsNotInitialised_fetchBrokerInfo_initialisesZookeeperConnection()
            throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());
        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(new ArrayList<ChildData>());

        brokerInfoFetcher.fetchBrokerInfo();

        Mockito.verify(zookeeperConnector).getCuratorFramework();
        Mockito.verify(pathChildrenCache).start(StartMode.BUILD_INITIAL_CACHE);

        brokerInfoFetcher.close();
    }

    @Test
    public void whenItIsInitialised_fetchBrokerInfo_reusesZookeeperConnection() throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());
        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(new ArrayList<ChildData>());

        brokerInfoFetcher.fetchBrokerInfo();
        brokerInfoFetcher.fetchBrokerInfo();

        Mockito.verify(brokerInfoFetcher, Mockito.times(1)).makePathChildrenCache();
        Mockito.verify(brokerInfoFetcher, Mockito.times(1)).makeZookeeperConnector(
                Mockito.anyString());

        brokerInfoFetcher.close();
    }

    @Test(expected = NoNodeException.class)
    public void whenPathDoesNotExist_fetchBrokerInfo_throwsAnException() throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(null);
        brokerInfoFetcher.fetchBrokerInfo();
    }

    @Test
    public void whenNoBrokersAtPath_fetchBrokerInfo_returnsAnEmptyList() throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());
        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(new ArrayList<ChildData>());

        List<BrokerInfo> brokerInfo = brokerInfoFetcher.fetchBrokerInfo();
        Assert.assertTrue(brokerInfo.isEmpty());
    }

    @Test
    public void whenOneBrokerAtPath_fetchBrokerInfo_returnsThatBroker() throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());

        ArrayList<ChildData> brokers = new ArrayList<ChildData>();
        ChildData childData = Mockito.mock(ChildData.class);
        Mockito.when(childData.getData()).thenReturn(new byte[] {});
        Mockito.when(childData.getPath()).thenReturn("/brokers/ids/1");

        brokers.add(childData);
        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(brokers);

        BrokerInfo dummyBroker = new BrokerInfo();
        Mockito.when(dataParser.parseData(Mockito.anyString())).thenReturn(dummyBroker);

        List<BrokerInfo> brokerInfo = brokerInfoFetcher.fetchBrokerInfo();
        Assert.assertEquals(1, brokerInfo.size());
        Assert.assertEquals(dummyBroker, brokerInfo.get(0));
    }

    @Test
    public void whenThreeBrokersAtPath_fetchBrokerInfo_returnsThoseBrokersInLexicographicOrderByBrokerId()
            throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());

        ArrayList<ChildData> brokers = new ArrayList<ChildData>();

        ChildData childData1 = Mockito.mock(ChildData.class);
        Mockito.when(childData1.getData()).thenReturn(new byte[] {});
        Mockito.when(childData1.getPath()).thenReturn("/brokers/ids/1");
        brokers.add(childData1);

        ChildData childData2 = Mockito.mock(ChildData.class);
        Mockito.when(childData2.getData()).thenReturn(new byte[] {});
        Mockito.when(childData2.getPath()).thenReturn("/brokers/ids/2");
        brokers.add(childData2);

        ChildData childData3 = Mockito.mock(ChildData.class);
        Mockito.when(childData3.getData()).thenReturn(new byte[] {});
        Mockito.when(childData3.getPath()).thenReturn("/brokers/ids/3");
        brokers.add(childData3);

        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(brokers);

        final BrokerInfo dummyBroker1 = new BrokerInfo();
        final BrokerInfo dummyBroker2 = new BrokerInfo();
        final BrokerInfo dummyBroker3 = new BrokerInfo();

        Mockito.when(dataParser.parseData(Mockito.anyString())).then(new Answer<BrokerInfo>() {

            private int counter = 0;

            public BrokerInfo answer(InvocationOnMock invocation) throws Throwable {
                BrokerInfo broker = null;
                switch (counter) {
                case 0:
                    broker = dummyBroker3;
                    break;
                case 1:
                    broker = dummyBroker1;
                    break;
                case 2:
                    broker = dummyBroker2;
                    break;
                }
                counter++;
                return broker;
            }
        });

        List<BrokerInfo> brokerInfo = brokerInfoFetcher.fetchBrokerInfo();

        Assert.assertEquals(3, brokerInfo.size());
        Assert.assertEquals(1, brokerInfo.get(0).getId());
        Assert.assertEquals(2, brokerInfo.get(1).getId());
        Assert.assertEquals(3, brokerInfo.get(2).getId());
    }

    @Test
    public void whenFiveBrokersAtPath_fetchBrokerInfo_returnsTopThreeBrokersInLexicographicOrderByBrokerId()
            throws Exception {
        Mockito.when(existsBuilder.forPath(Mockito.anyString())).thenReturn(new Stat());

        ArrayList<ChildData> brokers = new ArrayList<ChildData>();

        ChildData childData1 = Mockito.mock(ChildData.class);
        Mockito.when(childData1.getData()).thenReturn(new byte[] {});
        Mockito.when(childData1.getPath()).thenReturn("/brokers/ids/1");
        brokers.add(childData1);

        ChildData childData2 = Mockito.mock(ChildData.class);
        Mockito.when(childData2.getData()).thenReturn(new byte[] {});
        Mockito.when(childData2.getPath()).thenReturn("/brokers/ids/2");
        brokers.add(childData2);

        ChildData childData3 = Mockito.mock(ChildData.class);
        Mockito.when(childData3.getData()).thenReturn(new byte[] {});
        Mockito.when(childData3.getPath()).thenReturn("/brokers/ids/3");
        brokers.add(childData3);

        ChildData childData4 = Mockito.mock(ChildData.class);
        Mockito.when(childData4.getData()).thenReturn(new byte[] {});
        Mockito.when(childData4.getPath()).thenReturn("/brokers/ids/4");
        brokers.add(childData4);

        ChildData childData5 = Mockito.mock(ChildData.class);
        Mockito.when(childData5.getData()).thenReturn(new byte[] {});
        Mockito.when(childData5.getPath()).thenReturn("/brokers/ids/5");
        brokers.add(childData5);

        Mockito.when(pathChildrenCache.getCurrentData()).thenReturn(brokers);

        final BrokerInfo dummyBroker1 = new BrokerInfo();
        final BrokerInfo dummyBroker2 = new BrokerInfo();
        final BrokerInfo dummyBroker3 = new BrokerInfo();
        final BrokerInfo dummyBroker4 = new BrokerInfo();
        final BrokerInfo dummyBroker5 = new BrokerInfo();

        Mockito.when(dataParser.parseData(Mockito.anyString())).then(new Answer<BrokerInfo>() {

            private int counter = 0;

            public BrokerInfo answer(InvocationOnMock invocation) throws Throwable {
                BrokerInfo broker = null;
                switch (counter) {
                case 0:
                    broker = dummyBroker4;
                    break;
                case 1:
                    broker = dummyBroker1;
                    break;
                case 2:
                    broker = dummyBroker5;
                    break;
                case 3:
                    broker = dummyBroker2;
                    break;
                case 4:
                    broker = dummyBroker3;
                    break;
                }
                counter++;
                return broker;
            }
        });

        List<BrokerInfo> brokerInfo = brokerInfoFetcher.fetchBrokerInfo();

        Assert.assertEquals(3, brokerInfo.size());
        Assert.assertEquals(1, brokerInfo.get(0).getId());
        Assert.assertEquals(2, brokerInfo.get(1).getId());
        Assert.assertEquals(3, brokerInfo.get(2).getId());
    }
}
