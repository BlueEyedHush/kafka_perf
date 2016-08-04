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
        int topic, partition, max;

        if(topics > partitions) {
            topic = this.next;
            partition = generator.nextInt(partitions);
            max = topics;
        } else {
            topic = generator.nextInt(topics);
            partition = this.next;
            max = partitions;
        }

        this.next += this.increment;
        if(this.next > max) this.next = this.first;

        return new ProducerRecord<>(String.valueOf(topic), partition, null, msg);
    }
}
