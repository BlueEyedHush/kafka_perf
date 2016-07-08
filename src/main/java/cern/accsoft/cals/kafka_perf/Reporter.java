package cern.accsoft.cals.kafka_perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reporter.class);

    private final Collection<BlockingQueue<Long>> queues;
    private final int messageSize;
    private final int reps;
    private final Consumer<Long> throughputConsumer;

    public Reporter(Reportee reportee, int messageSize, int reps, Consumer<Long> throughputConsumer) {
        this.messageSize = messageSize;
        this.reps = reps;
        this.throughputConsumer = throughputConsumer;
        this.queues = reportee.getResults().values();
    }

    public void startReporting() {
        final int probeNumber = queues.size();

        while(true) {
            long summaryTime = 0;

            for(BlockingQueue<Long> q: queues) {
                try {
                    summaryTime += q.take();
                } catch (InterruptedException e) {
                    LOGGER.error("Unexpected InterruptedException", e);
                }
            }

            long throughput = probeNumber*reps*messageSize*1000/summaryTime; /* in B/s */
            throughputConsumer.accept(throughput);
        }
    }
}
