package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplierProducer;
import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.SingleTopicFixedLengthSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.StrippingSupplier;
import cern.accsoft.cals.kafka_perf.reporters.FileReporter;
import com.martiansoftware.jsap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 */
public class App {
    public static final String RESULTS_FILE_PATH = "/tmp/results";

    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    private static final String THREADS_OPT = "threads";
    private static final String MESSAGE_SUPPLIER_ID_OPT = "m_sup";
    private static final String THROTTLE_OPT = "throttle";

    private static final String PARTITION_OPT = "partition";
    private static final String PARTITIONING_DISABLED = "0,1";
    private static final Pattern PARTITIONING_REGEX = Pattern.compile("([0-9]+),([0-9]+)");

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
                                "throttle to ~ messages/second"),
                        new FlaggedOption(PARTITION_OPT,
                                JSAP.STRING_PARSER,
                                PARTITIONING_DISABLED,
                                JSAP.NOT_REQUIRED,
                                'p',
                                JSAP.NO_LONGFLAG,
                                "which partition of topics this thread is responsible for")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        return config;
    }

    private void start(JSAPResult config) throws Exception { /* Exception from coordinator.run() */
        final int threads = config.getInt(THREADS_OPT);
        final String messageSupplierId = config.getString(MESSAGE_SUPPLIER_ID_OPT);
        final long messagesPerSecond = config.getLong(THROTTLE_OPT);
        final String partitioning = config.getString(PARTITION_OPT);

        int id = -1;
        int all = -1;
        Matcher m = PARTITIONING_REGEX.matcher(partitioning);
        if(m.matches()) {
            id = Integer.valueOf(m.group(1));
            all = Integer.valueOf(m.group(2));
        }

        List<BenchmarkingService> benchmarkingServiceList = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            MessageSupplierProducer producer;
            switch (messageSupplierId.toLowerCase()) {
                case "mtfl":
                    producer = MultipleTopicFixedLenghtSupplier::new;
                    break;
                case "stfl":
                    producer = SingleTopicFixedLengthSupplier::new;
                    break;
                case "ss":
                    int thread = i, _id = id, _all = all;
                    producer = (messageLength, topicNumber, partitionNumber) -> {
                        int[] coords = StrippingSupplier.getFirstAndIncrement(_id, _all, thread, threads);
                        return new StrippingSupplier(messageLength, coords[0], coords[1], topicNumber, partitionNumber);
                    };
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized id: ".concat(messageSupplierId));
            }

            BenchmarkingService service =
                    BenchmarkingService.spawnAndStartBenchmarkingService(producer, messagesPerSecond);
            benchmarkingServiceList.add(service);
        }

        FileReporter r = new FileReporter(Paths.get(RESULTS_FILE_PATH));
        BenchmarkCoordinator coordinator = new BenchmarkCoordinator(benchmarkingServiceList, r);
        coordinator.run();
    }
}
