package cern.accsoft.cals.kafka_perf;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Config {
    public static final Map<String, Object> KAFKA_CONFIGURATION;

    static {
        Map<String, Object> m = new HashMap<>();

        m.put("bootstrap.servers", "128.142.128.88:9092,128.142.134.233:9092");
        m.put("key.serializer", StringSerializer.class);
        m.put("value.serializer", StringSerializer.class);
        m.put("partitioner.class", FairPartitioner.class);

        KAFKA_CONFIGURATION = Collections.unmodifiableMap(m);
    }

    public static final String ZK_CONNECTION_STRING = "188.184.165.208:2181";
    public static final RetryPolicy ZK_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);
    public static final String TEST_ZNODE_PATH = "/kafka_perf_test";
}
