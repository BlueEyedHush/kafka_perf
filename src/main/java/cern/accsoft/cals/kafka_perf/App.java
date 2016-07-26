package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.reporters.FileReporter;
import com.martiansoftware.jsap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static final String RESULTS_FILE_PATH = "/tmp/results";

    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    private static final String THREADS_OPT = "threads";
    private static final String MESSAGE_SUPPLIER_ID_OPT = "m_sup";
    private static final String THROTTLE_OPT = "throttle";

    public static void main(String[] args) throws Exception {
        LOGGER.info("Application started");
        new App().start(getCliOptions(args));
        LOGGER.info("Application terminated");
    }

    private static JSAPResult getCliOptions(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP("Kafka testing daemon", "",
                new Parameter[]{
                        new FlaggedOption(THREADS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 't', JSAP.NO_LONGFLAG,
                                "Number of threads sending messages."),
                        new FlaggedOption(MESSAGE_SUPPLIER_ID_OPT, JSAP.STRING_PARSER, "mtfl", JSAP.NOT_REQUIRED, 's', JSAP.NO_LONGFLAG,
                                "ID of message supplier to use"),
                        new FlaggedOption(THROTTLE_OPT,
                                JSAP.LONG_PARSER,
                                Long.toString(Throttler.THROTTLING_DISABLED),
                                JSAP.NOT_REQUIRED,
                                'T',
                                JSAP.NO_LONGFLAG,
                                "throttle to ~ messages/second")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        return config;
    }

    private void start(JSAPResult config) throws Exception { /* Exception from coordinator.run() */
        final int threads = config.getInt(THREADS_OPT);
        final String messageSupplierId = config.getString(MESSAGE_SUPPLIER_ID_OPT);
        final long messagesPerSecond = config.getShort(THROTTLE_OPT);

        List<BenchmarkingService> benchmarkingServiceList = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            BenchmarkingService service =
                    BenchmarkingService.spawnAndStartBenchmarkingService(messageSupplierId, messagesPerSecond);
            benchmarkingServiceList.add(service);
        }

        FileReporter r = new FileReporter(Paths.get(RESULTS_FILE_PATH));
        BenchmarkCoordinator coordinator = new BenchmarkCoordinator(benchmarkingServiceList, r);
        coordinator.run();
    }
}
