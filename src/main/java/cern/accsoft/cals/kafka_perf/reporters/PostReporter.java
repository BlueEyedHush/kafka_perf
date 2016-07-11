package cern.accsoft.cals.kafka_perf.reporters;

import cern.accsoft.cals.kafka_perf.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This reporter is run after all of the probes finish
 */
public class PostReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostReporter.class);

    private final Collection<List<Long>> lists;
    private final int messageSize;
    private final int reps;

    public PostReporter(Collector collector, int messageSize, int reps) {
        this.messageSize = messageSize;
        this.reps = reps;
        this.lists = collector.getResults().values();
    }

    public void report() {
        double totalMean = 0.0;
        /* variance is calculated assuming, that threads doesn't influence one another, which is obviously false,
         * so results might not be representative, but trends should be (since all manipulation is done server-,
          * not client-side */
        double totalVariance = 0.0;

        for(List<Long> q: lists) {
            String throughtputs = q.stream()
                    .map((time) -> calculateThroughput(time, reps, messageSize))
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            System.out.println(throughtputs);
        }
    }

    /**
     * @return throughput in B/s
     */
    private static Double calculateThroughput(long time, int reps, int messageSize) {
        return ((double) reps) * messageSize * 1000 / time;
    }
}
