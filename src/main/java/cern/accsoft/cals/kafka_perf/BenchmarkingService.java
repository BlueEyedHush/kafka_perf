package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.concurrent.Semaphore;

public class BenchmarkingService implements Runnable {
    public static BenchmarkingService spawnAndStartBenchmarkingService() {
        BenchmarkingService bs = new BenchmarkingService();

        Thread t = new Thread(bs, "benchmarking_thread");
        t.setDaemon(true);
        t.start();

        return bs;
    }

    private Semaphore semaphore;
    private MessageSupplier messageSupplier;
    private volatile long message_count = 0;

    public BenchmarkingService() {
        semaphore = new Semaphore(1, true);
        semaphore.acquireUninterruptibly();
    }

    /**
     * Called from another thread
     * Must be called pairwise with stop!
     */
    public void startTest(int messageSize, int topicCount) {
        /* is this safe */
        messageSupplier = new MultipleTopicFixedLenghtSupplier(messageSize, topicCount);
        message_count = 0;
        semaphore.release();
    }

    /**
     * Called from another thread
     */
    public long stopTestAndReturnResults() {
        semaphore.acquireUninterruptibly();
        return message_count;
    }

    @Override
    public void run() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Config.KAFKA_CONFIGURATION)) {
            while(true) {
                /* while we are OK to test, semaphore is down - but when stop command arrives, other thread will still it
                 * and won't release it until time for a new session arrives */
                semaphore.acquireUninterruptibly();
                try {
                    producer.send(messageSupplier.get());
                    message_count++;
                } finally {
                    semaphore.release();
                }
            }
        }
    }
}
