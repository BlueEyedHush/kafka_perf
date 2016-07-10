package cern.accsoft.cals.kafka_perf.reporters;

import cern.accsoft.cals.kafka_perf.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class LiveReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveReporter.class);

    private final Collection<BlockingQueue<Long>> queues;
    private final int messageSize;
    private final int reps;
    private final Consumer<Double> throughputConsumer;

    public LiveReporter(Collector collector, int messageSize, int reps, Consumer<Double> throughputConsumer) {
        this.messageSize = messageSize;
        this.reps = reps;
        this.throughputConsumer = throughputConsumer;
        this.queues = collector.getResults().values();
    }

    public void startReporting() {
        final int numberOfProbes = queues.size();

        while(true) {
            double summaryTime = 0;

            for(BlockingQueue<Long> q: queues) {
                try {
                    summaryTime += q.take();
                } catch (InterruptedException e) {
                    LOGGER.error("Unexpected InterruptedException", e);
                }
            }

            double throughput = 0;
            if(summaryTime != 0) {
                throughput = ((double) numberOfProbes) * reps * messageSize * 1000 / summaryTime; /* in B/s */
            }
            throughputConsumer.accept(throughput);
        }
    }
}
