package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplier;
import cern.accsoft.cals.kafka_perf.message_suppliers.MessageSupplierFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkingService implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkingService.class.getSimpleName());
    private static final long NOT_REPORTED = -1;

    public static BenchmarkingService spawnAndStartBenchmarkingService(String supplierId) {
        BenchmarkingService bs = new BenchmarkingService(supplierId);

        Thread t = new Thread(bs, "benchmarking_thread");
        t.setDaemon(true);
        t.start();

        return bs;
    }

    private final String supplierId;
    private Semaphore semaphore;
    private MessageSupplier messageSupplier;
    private volatile long messageCount = 0;

    private volatile long startTimestamp;
    private AtomicLong endTimestamp;

    public BenchmarkingService(String supplierId) {
        this.supplierId = supplierId;
        semaphore = new Semaphore(1, true);
        semaphore.acquireUninterruptibly();
    }

    /**
     * Called from another thread
     * Must be called pairwise with stop!
     */
    public void startTest(int messageSize, int topicCount, int partitionsPerTopic) {
        /* is this safe */
        messageSupplier = MessageSupplierFactory.get(supplierId, messageSize, topicCount, partitionsPerTopic);
        messageCount = 0;
        startTimestamp = System.currentTimeMillis();
        endTimestamp = new AtomicLong(NOT_REPORTED);

        semaphore.release();
    }

    /**
     * Called from another thread
     */
    public long getResults() {
        return messageCount;
    }

    public long getLag() {
        return endTimestamp.get() - startTimestamp;
    }

    public void stop() {
        semaphore.acquireUninterruptibly();
    }

    @Override
    public void run() {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Config.KAFKA_CONFIGURATION)) {
            while(true) {
                semaphore.acquireUninterruptibly();
                /* while we are OK to test, semaphore is down - but when stop command arrives, other thread will still it
                 * and won't release it until time for a new session arrives */
                try {
                    producer.send(messageSupplier.get(), (metadata, ex) -> {
                        if(metadata != null) {
                            endTimestamp.compareAndSet(NOT_REPORTED, System.currentTimeMillis());
                            messageCount++;
                        } else {
                            LOGGER.error("Message processing error: ", ex);
                        }
                    });
                } finally {
                    semaphore.release();
                }
            }
        }
    }
}
