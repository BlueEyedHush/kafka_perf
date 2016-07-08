package cern.accsoft.cals.kafka_perf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Configuration {
    public static final Map<String, Object> KAFKA_CONFIGURATION;

    static {
        Map<String, Object> m = new HashMap<>();

        m.put("bootstrap.servers", "128.142.128.88:9092,128.142.134.233:9092");

        KAFKA_CONFIGURATION = Collections.unmodifiableMap(m);
    }
}