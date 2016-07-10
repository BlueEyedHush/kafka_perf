package cern.accsoft.cals.kafka_perf.reporters;

import cern.accsoft.cals.kafka_perf.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * This reporter is run after all of the probes finish
 */
public class PostReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostReporter.class);

    private final Collection<BlockingQueue<Long>> queues;
    private final int messageSize;
    private final int reps;
    private final Consumer<Double> throughputConsumer;

    public PostReporter(Collector collector, int messageSize, int reps, Consumer<Double> throughputConsumer) {
        this.messageSize = messageSize;
        this.reps = reps;
        this.throughputConsumer = throughputConsumer;
        this.queues = collector.getResults().values();
    }

    public void report() {
        final int numberOfProbes = queues.size();

        if(numberOfProbes < 1) {
            throw new IllegalStateException("At least one probe must be used");
        }

        for(int i = 0; i < queues.iterator().next().size(); i++) {
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
