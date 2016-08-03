package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class StrippingSupplier implements MessageSupplier {

    public static int[] getFirstAndIncrement(int id, int all, int thread, int threads_all) {
        int increment = all*threads_all;
        int first = id + thread*all;

        return new int[]{first, increment};
    }

    private final byte[] msg;
    private final int first;
    private final int increment;
    private final int topics;
    private final int partitions;
    private final Random generator = new Random();

    private int next;

    public StrippingSupplier(int messageLength, int first, int increment, int topics, int partitions) {
        this.first = first;
        this.increment = increment;
        this.topics = topics;
        this.partitions = partitions;
        this.msg = Utils.generateWithLength(messageLength);

        this.next = this.first;
    }

    @Override
    public ProducerRecord<byte[], byte[]> get() {
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(String.valueOf(this.next), generator.nextInt(partitions), null, msg);
        this.next += this.increment;
        if(this.next > this.topics) this.next = this.first;

        return record;
    }
}
