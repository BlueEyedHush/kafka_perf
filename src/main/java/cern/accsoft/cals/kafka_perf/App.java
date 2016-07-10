package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.collectors.TimingCollector;
import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.SingleTopicFixedLengthSupplier;
import cern.accsoft.cals.kafka_perf.printers.PrettyThroughputPrinter;
import cern.accsoft.cals.kafka_perf.printers.RawThroughtputPrinter;
import com.martiansoftware.jsap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Hello world!
 */
public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    private static final String REPS_OPT = "reps";
    private static final String SERIES_OPT = "series";
    private static final String THREADS_OPT = "threads";
    private static final String RAW_REPORTER_OPT = "raw_rep";
    private static final String TOPICS_OPT = "topics";

    public static void main(String[] args) throws Exception {
        LOGGER.info("Application started");
        new App().start(getCliOptions(args));
        LOGGER.info("Application terminated");
    }

    private static JSAPResult getCliOptions(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP("Kafka performance tester", "",
                new Parameter[]{
                        new FlaggedOption(SERIES_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 's', JSAP.NO_LONGFLAG,
                                "Number of test series. Duration of the whole series is timed."),
                        new FlaggedOption(REPS_OPT, JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', JSAP.NO_LONGFLAG,
                                "Number of repetitions in each series"),
                        new FlaggedOption(THREADS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 't', JSAP.NO_LONGFLAG,
                                "Number of threads sending messages"),
                        new Switch(RAW_REPORTER_OPT, 'R', JSAP.NO_LONGFLAG,
                                "Should raw throughput printer be used instead of pretty"),
                        new FlaggedOption(TOPICS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'T', JSAP.NO_LONGFLAG,
                                "Number of topics to which messages will be sent")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        return config;
    }

    private void start(JSAPResult config) {
        final int series = config.getInt(SERIES_OPT);
        final int reps = config.getInt(REPS_OPT);
        final int threads = config.getInt(THREADS_OPT);
        final Consumer<Double> printer = config.getBoolean(RAW_REPORTER_OPT) ?
                new RawThroughtputPrinter() : new PrettyThroughputPrinter();
        final int topics = config.getInt(TOPICS_OPT);

        /* Kafka's default serialzier uses UTF8, so it should give 0.5kB */
        final String msgBody = Utils.generateWithLength(512);

        TimingCollector c = new TimingCollector();
        Reporter r = new Reporter(c, 3, reps, printer);

        for (int i = 0; i < threads; i++) {
            BenchmarkingProducer.createAndSpawnOnNewThread(new MultipleTopicFixedLenghtSupplier(512, topics),
                    topics,
                    reps,
                    series,
                    c.createProbe());
        }

        /* blocking */
        r.startReporting();
    }

}
