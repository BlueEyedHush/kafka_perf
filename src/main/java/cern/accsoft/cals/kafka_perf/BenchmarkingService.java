package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;

public class BenchmarkingService implements Runnable {
    public static BenchmarkingService spawnAndStartBenchmarkingService() {
        BenchmarkingService bs = new BenchmarkingService();

        Thread t = new Thread(bs, "benchmarking_thread");
        t.setDaemon(true);
        t.start();

        return bs;
    }

    private MessageSupplier messageSupplier;
    private volatile boolean running = false;
    private volatile long message_count = 0;

    public BenchmarkingService() {}

    /**
     * Called from another thread
     */
    public void startTest(int messageSize, int topicCount) {
        if(running) throw new IllegalStateException("Test is already running!");

        /* is this safe */
        messageSupplier = new MultipleTopicFixedLenghtSupplier(messageSize, topicCount);
        message_count = 0;
        /* write to volatile - should create memory barrier, therefore neither assignments of above fields, nor
         * writes which initialize MultipleTopic... can be reordered beyond that point */
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
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Config.KAFKA_CONFIGURATION)) {
            while(true) {
                /* benchmark */
                if(running) {
                    message_count = benchmark(producer);
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
