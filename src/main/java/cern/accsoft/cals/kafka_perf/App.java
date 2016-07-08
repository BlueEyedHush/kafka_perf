package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.collectors.TimingCollector;
import cern.accsoft.cals.kafka_perf.printers.PrettyThroughputPrinter;
import com.martiansoftware.jsap.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    private static final String REPS_OPT = "reps";
    private static final String SERIES_OPT = "series";
    private static final String THREADS_OPT = "threads";

    public static void main( String[] args ) throws Exception {
        LOGGER.info("Application started");
        new App().start(getCliOptions(args));
        LOGGER.info("Application terminated");
    }

    private static JSAPResult getCliOptions(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP("Kafka performance tester", "",
                new Parameter[] {
                        new FlaggedOption( SERIES_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 's', JSAP.NO_LONGFLAG,
                                "Number of test series. Duration of the whole series is timed." ),
                        new FlaggedOption( REPS_OPT, JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', JSAP.NO_LONGFLAG,
                                "Number of repetitions in each series" ),
                        new FlaggedOption( THREADS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 't', JSAP.NO_LONGFLAG,
                                "Number of threads sending messages" )
                }
        );

        JSAPResult config = jsap.parse(args);
        if(jsap.messagePrinted()) System.exit(1);

        return config;
    }

    private void start(JSAPResult config) {
        final int series = config.getInt(SERIES_OPT);
        final int reps = config.getInt(REPS_OPT);
        final int threads = config.getInt(THREADS_OPT);

        TimingCollector c = new TimingCollector();
        Reporter r = new Reporter(c, 3, reps, new PrettyThroughputPrinter());

        for(int i = 0; i < threads; i++) {
            BenchmarkingProducer.createAndSpawnOnNewThread(() -> new ProducerRecord<String, String>("test_topic", "MSG"),
                    reps,
                    series,
                    c.createProbe());
        }

        /* blocking */
        r.startReporting();
    }

}
