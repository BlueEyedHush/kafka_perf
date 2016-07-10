package cern.accsoft.cals.kafka_perf.collectors;

import cern.accsoft.cals.kafka_perf.Collector;
import cern.accsoft.cals.kafka_perf.Probe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimingCollector implements Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimingCollector.class);

    private class TimingProbe implements Probe {

        private final int id;
        private long startTimestamp;

        public TimingProbe(int id) {
            this.id = id;
        }

        @Override
        public void beforeSeries() {
            startTimestamp = System.currentTimeMillis();
        }

        @Override
        public void afterSeries() {
            long diff = System.currentTimeMillis() - startTimestamp;
            reportResult(id, diff);
        }
    }

    private int nextProbeId = 0;
    /* map cannot be changed after benchmarking is started - keys and values may stay the same (although state
     * of the values might change) */
    private Map<Integer, List<Long>> probeToResultQueue = new HashMap<>();

    public TimingCollector() {}

    /* can be called only before the benchmarking is started */
    public TimingProbe createProbe() {
        int id = nextProbeId++;
        probeToResultQueue.put(id, new LinkedList<>());
        return this.new TimingProbe(id);
    }

    @Override
    public Map<Integer, List<Long>> getResults() {
        return Collections.unmodifiableMap(probeToResultQueue);
    }

    private void reportResult(int probeId, long diff) {
        probeToResultQueue.get(probeId).add(diff);
    }
}
