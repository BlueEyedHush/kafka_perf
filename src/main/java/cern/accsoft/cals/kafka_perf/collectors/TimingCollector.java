package cern.accsoft.cals.kafka_perf.collectors;

import cern.accsoft.cals.kafka_perf.Collector;
import cern.accsoft.cals.kafka_perf.Probe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private Map<Integer, BlockingQueue<Long>> probeToResultQueue = new HashMap<>();

    public TimingCollector() {}

    /* can be called only before the benchmarking is started */
    public TimingProbe getProbe() {
        int id = nextProbeId++;
        probeToResultQueue.put(id, new LinkedBlockingQueue<>());
        return this.new TimingProbe(id);
    }

    @Override
    public Map<Integer, BlockingQueue<Long>> getResults() {
        return Collections.unmodifiableMap(probeToResultQueue);
    }

    private void reportResult(int probeId, long diff) {
        try {
            probeToResultQueue.get(probeId).offer(diff, 10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("unexpected InterruptedException", e);
        }
    }
}
