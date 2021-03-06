package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.MultipleTopicFixedLenghtSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.concurrent.Semaphore;

public class BenchmarkingService implements Runnable {
    private final int partitionsPerTopic;

    public static BenchmarkingService spawnAndStartBenchmarkingService(int partitionsPerTopic) {
        BenchmarkingService bs = new BenchmarkingService(partitionsPerTopic);

        Thread t = new Thread(bs, "benchmarking_thread");
        t.setDaemon(true);
        t.start();

        return bs;
    }

    private Semaphore semaphore;
    private MessageSupplier messageSupplier;
    private volatile long messageCount = 0;

    public BenchmarkingService(int partitionsPerTopic) {
        this.partitionsPerTopic = partitionsPerTopic;
        semaphore = new Semaphore(1, true);
        semaphore.acquireUninterruptibly();
    }

    /**
     * Called from another thread
     * Must be called pairwise with stop!
     */
    public void startTest(int messageSize, int topicCount) {
        /* is this safe */
        messageSupplier = new MultipleTopicFixedLenghtSupplier(messageSize, topicCount, partitionsPerTopic);
        messageCount = 0;
        semaphore.release();
    }

    /**
     * Called from another thread
     */
    public long getMessageCount() {
        return messageCount;
    }

    public void stop() {
        semaphore.acquireUninterruptibly();
    }

    @Override
    public void run() {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Config.KAFKA_CONFIGURATION)) {
            while(true) {
                /* while we are OK to test, semaphore is down - but when stop command arrives, other thread will still it
                 * and won't release it until time for a new session arrives */
                semaphore.acquireUninterruptibly();
                try {
                    producer.send(messageSupplier.get());
                    messageCount++;
                } finally {
                    semaphore.release();
                }
            }
        }
    }
}
