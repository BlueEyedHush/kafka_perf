package cern.accsoft.cals.kafka_perf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Supplier;

public class BenchmarkingService implements Runnable {
    public static BenchmarkingService spawnAndStartBenchmarkingService(Supplier<ProducerRecord<String, String>> messageSupplier) {
        BenchmarkingService bs = new BenchmarkingService(messageSupplier);

        Thread t = new Thread(bs, "benchmarking_thread");
        t.setDaemon(true);
        t.start();

        return bs;
    }

    private final Supplier<ProducerRecord<String, String>> messageSupplier;

    private volatile boolean running = false;
    private volatile long message_count = 0;

    public BenchmarkingService(Supplier<ProducerRecord<String, String>> messageSupplier) {
        this.messageSupplier = messageSupplier;
    }

    /**
     * Called from another thread
     */
    public void startTest() {
        if(running) throw new IllegalStateException("Test is already running!");

        message_count = 0;
        running = true;
    }

    /**
     * Called from another thread
     */
    public long stopTestAndReturnResults() {
        if(!running) throw new IllegalStateException("Test wasn't running, so can't stop it!");

        running = false;
        return message_count;
    }

    @Override
    public void run() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Configuration.KAFKA_CONFIGURATION)) {
            while(true) {
                /* benchmark */
                if(running) {
                    long message_count = benchmark(producer);
                }
            }
        }
    }

    private long benchmark(KafkaProducer<String, String> producer) {
        long message_count = 0;

        while(running) {
            producer.send(messageSupplier.get());
            message_count++;
        }

        return message_count;
    }
}
