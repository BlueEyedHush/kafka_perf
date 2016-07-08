package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.collectors.TimingCollector;
import cern.accsoft.cals.kafka_perf.printers.PrettyThroughputPrinter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    public static void main( String[] args ) {
        LOGGER.info("Application started");
        new App().start();
        LOGGER.info("Application terminated");
    }

    private void start() {
        final int reps = 1_000_000;

        TimingCollector c = new TimingCollector();
        Reporter r = new Reporter(c, 3, reps, new PrettyThroughputPrinter());

        BenchmarkingProducer.createAndSpawnOnNewThread(() -> new ProducerRecord<String, String>("test_topic", "MSG"),
                reps, 10, c.getProbe());

        /* blocking */
        r.startReporting();
    }
}
