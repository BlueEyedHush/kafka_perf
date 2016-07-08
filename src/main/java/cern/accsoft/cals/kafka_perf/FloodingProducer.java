package cern.accsoft.cals.kafka_perf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class FloodingProducer implements Runnable {

    private final Supplier<ProducerRecord<String, String>> messageSupplier;
    private final int reps;
    private final int series;
    private final ResultCollector resultCollector;

    public FloodingProducer(Supplier<ProducerRecord<String, String>> messageSupplier,
                            int reps,
                            int series,
                            ResultCollector resultCollector) {
        this.messageSupplier = messageSupplier;
        this.reps = reps;
        this.series = series;
        this.resultCollector = resultCollector;
    }

    @Override
    public void run() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Configuration.KAFKA_CONFIGURATION);

        /* warmup */
        runReps(producer, reps);

        /* benchmark */
        for(int i = 0; i < series; i++) {
            resultCollector.beforeSeries();
            runReps(producer, reps);
            resultCollector.afterSeries();
        }
    }

    private void runReps(KafkaProducer<String, String> producer, int reps) {
        for(int i = 0; i < reps; i++) {
            producer.send(messageSupplier.get());
        }
    }
}
