package cern.accsoft.cals.kafka_perf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Supplier;

public class BenchmarkingProducer implements Runnable {
    private static final int WARMUP_REPS = 100;

    public static void createAndSpawnOnNewThread(Supplier<ProducerRecord<String, String>> messageSupplier,
                                                 int reps,
                                                 int series,
                                                 Probe probe,
                                                 Runnable onFinish) {
        Thread t = new Thread(new BenchmarkingProducer(messageSupplier, reps, series, probe, onFinish));
        t.start();
    }

    private final Supplier<ProducerRecord<String, String>> messageSupplier;
    private final int reps;
    private final int series;
    private final Probe probe;
    private final Runnable onFinish;

    public BenchmarkingProducer(Supplier<ProducerRecord<String, String>> messageSupplier,
                                int reps,
                                int series,
                                Probe probe,
                                Runnable onFinish) {
        this.messageSupplier = messageSupplier;
        this.reps = reps;
        this.series = series;
        this.probe = probe;
        this.onFinish = onFinish;
    }

    @Override
    public void run() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Configuration.KAFKA_CONFIGURATION)) {
            /* warmup */
            runReps(producer, WARMUP_REPS);

            /* benchmark */
            for (int i = 0; i < series; i++) {
                probe.beforeSeries();
                runReps(producer, reps);
                probe.afterSeries();
            }
        }

        onFinish.run();
    }

    private void runReps(KafkaProducer<String, String> producer, int reps) {
        for (int i = 0; i < reps; i++) {
            producer.send(messageSupplier.get());
        }
    }
}
