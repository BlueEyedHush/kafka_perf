package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import cern.accsoft.cals.kafka_perf.reporters.FileReporter;
import com.martiansoftware.jsap.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private static final String TOPICS_OPT = "topics";
    private static final String TOPIC_CREATION_MODE_OPT = "tc_mode";

    public static void main(String[] args) throws Exception {
        LOGGER.info("Application started");
        new App().start(getCliOptions(args));
        LOGGER.info("Application terminated");
    }

    private static JSAPResult getCliOptions(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP("Kafka testing daemon", "",
                new Parameter[]{
                        new FlaggedOption(THREADS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 't', JSAP.NO_LONGFLAG,
                                "Number of threads sending messages"),
                        new FlaggedOption(TOPICS_OPT, JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'T', JSAP.NO_LONGFLAG,
                                "Number of topics to which messages will be sent. Only relevant in topic creation mode."),
                        new Switch(TOPIC_CREATION_MODE_OPT, 'c', JSAP.NO_LONGFLAG,
                                "Create required topics instead of benchmarking")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        return config;
    }

    private void start(JSAPResult config) throws Exception { /* Exception from coordinator.run() */
        final int topics = config.getInt(TOPICS_OPT);
        final int threads = config.getInt(THREADS_OPT);

        if(!config.getBoolean(TOPIC_CREATION_MODE_OPT)) {
            List<BenchmarkingService> benchmarkingServiceList = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                BenchmarkingService service = BenchmarkingService
                        .spawnAndStartBenchmarkingService(new MultipleTopicFixedLenghtSupplier(500, topics));
                benchmarkingServiceList.add(service);
            }

            FileReporter r = new FileReporter(Paths.get(RESULTS_FILE_PATH));
            BenchmarkCoordinator coordinator = new BenchmarkCoordinator(benchmarkingServiceList, r);
            coordinator.run();
        } else {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Config.KAFKA_CONFIGURATION)) {
                for(int i = 0; i < topics; i++) {
                    producer.send(new ProducerRecord<>(String.valueOf(i), "tc"));
                }
            }
        }
    }

}
