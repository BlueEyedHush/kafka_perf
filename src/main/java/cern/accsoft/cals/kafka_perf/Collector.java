package cern.accsoft.cals.kafka_perf;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public interface Collector {
    /**
     * Should always return the same, so that reporter can cache it
     */
    Map<Integer, BlockingQueue<Long>> getResults();
}
