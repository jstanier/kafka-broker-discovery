package com.brandwatch.kafka.discovery;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class ConnectionStringCreator {

    private static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();
    private static final Joiner COLON_JOINER = Joiner.on(":").skipNulls();

    public String createConnectionString(List<BrokerInfo> brokers) {
        Preconditions.checkNotNull(brokers);
        String result = null;
        for (BrokerInfo brokerInfo : brokers) {
            String oneConnectionString = COLON_JOINER.join(brokerInfo.getHost(),
                    brokerInfo.getPort());
            result = COMMA_JOINER.join(result, oneConnectionString);
        }
        return result;
    }

}
