package cern.accsoft.cals.kafka_perf;

import java.util.List;
import java.util.Map;

public interface Collector {
    /**
     * Should always return the same, so that reporter can cache it
     */
    Map<Integer, List<Long>> getResults();
}
