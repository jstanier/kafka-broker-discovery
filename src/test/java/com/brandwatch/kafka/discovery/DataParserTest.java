package com.brandwatch.kafka.discovery;

import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataParserTest {

    private DataParser dataParser;

    @Before
    public void setup() {
        dataParser = new DataParser();
    }

    @Test(expected = NullPointerException.class)
    public void givenANullDataString_parseData_throwsANullPointerException()
            throws BrokerParserException {
        dataParser.parseData(null);
    }

    @Test(expected = BrokerParserException.class)
    public void givenADataStringWithMissingOpeningBrace_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        dataParser.parseData(" \"a\":\"b\" }");
    }

    @Test(expected = BrokerParserException.class)
    public void givenADataStringWithMissingClosingBrace_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        dataParser.parseData("{ \"a\":\"b\" ");
    }

    @Test(expected = BrokerParserException.class)
    public void givenAnEmptyDataString_parseData_returnsABrokerParserException()
            throws BrokerParserException {
        dataParser.parseData("");
    }

    @Test
    public void givenAStringWithOneBroker_parseData_returnsAValidBrokerInfo()
            throws BrokerParserException {
        String validBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\", \"version\":1 }";
        BrokerInfo brokerInfo = dataParser.parseData(validBrokerConfiguration);
        Assert.assertEquals("beetlejuice.runtime-collective.com", brokerInfo.getHost());
        Assert.assertEquals(9093, brokerInfo.getJmxPort());
        Assert.assertEquals(9092, brokerInfo.getPort());
        Assert.assertEquals(1, brokerInfo.getVersion());
        Assert.assertEquals(new Date(1424095336398L), brokerInfo.getTimestamp());
    }

    @Test
    public void givenAStringWithOneBrokerWithExtraLeadingWhitespace_parseData_returnsAValidBrokerInfo()
            throws BrokerParserException {
        String validBrokerConfiguration = "    { \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\", \"version\":1 }";
        BrokerInfo brokerInfo = dataParser.parseData(validBrokerConfiguration);
        Assert.assertEquals("beetlejuice.runtime-collective.com", brokerInfo.getHost());
        Assert.assertEquals(9093, brokerInfo.getJmxPort());
        Assert.assertEquals(9092, brokerInfo.getPort());
        Assert.assertEquals(1, brokerInfo.getVersion());
        Assert.assertEquals(new Date(1424095336398L), brokerInfo.getTimestamp());
    }

    @Test
    public void givenAStringWithOneBrokerWithExtraTrailingWhitespace_parseData_returnsAValidBrokerInfo()
            throws BrokerParserException {
        String validBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\", \"version\":1 }     ";
        BrokerInfo brokerInfo = dataParser.parseData(validBrokerConfiguration);
        Assert.assertEquals("beetlejuice.runtime-collective.com", brokerInfo.getHost());
        Assert.assertEquals(9093, brokerInfo.getJmxPort());
        Assert.assertEquals(9092, brokerInfo.getPort());
        Assert.assertEquals(1, brokerInfo.getVersion());
        Assert.assertEquals(new Date(1424095336398L), brokerInfo.getTimestamp());
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithOneBrokerWithAMissingHost_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\", \"version\":1 }";
        dataParser.parseData(invalidBrokerConfiguration);
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithOneBrokerWithAMissingPort_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"timestamp\":\"1424095336398\", \"version\":1 }";
        dataParser.parseData(invalidBrokerConfiguration);
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithOneBrokerWithAMissingJmxPort_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"port\":9092, \"timestamp\":\"1424095336398\", \"version\":1 }";
        dataParser.parseData(invalidBrokerConfiguration);
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithOneBrokerWithAMissingTimestamp_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"version\":1 }";
        dataParser.parseData(invalidBrokerConfiguration);
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithOneBrokerWithAMissingVersion_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\" }";
        dataParser.parseData(invalidBrokerConfiguration);
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithAMissingKey_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ : \"beetlejuice.runtime-collective.com\", \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\" }";
        BrokerInfo parseData = dataParser.parseData(invalidBrokerConfiguration);
        System.out.println(parseData.getHost());
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithAMissingValue_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\": , \"jmx_port\":9093, \"port\":9092, \"timestamp\":\"1424095336398\" }";
        BrokerInfo parseData = dataParser.parseData(invalidBrokerConfiguration);
        System.out.println(parseData.getHost());
    }

    @Test(expected = BrokerParserException.class)
    public void givenAStringWithAMissingKeyValuePair_parseData_throwsABrokerParserException()
            throws BrokerParserException {
        String invalidBrokerConfiguration = "{ \"host\":\"beetlejuice.runtime-collective.com\" , , \"port\":9092, \"timestamp\":\"1424095336398\" }";
        BrokerInfo parseData = dataParser.parseData(invalidBrokerConfiguration);
        System.out.println(parseData.getHost());
    }
}
