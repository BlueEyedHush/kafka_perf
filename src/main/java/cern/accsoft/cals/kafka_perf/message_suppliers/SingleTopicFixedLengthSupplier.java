package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

/**
 * Created by knawara on 7/19/16.
 */
public class SingleTopicFixedLengthSupplier implements MessageSupplier {
    private static final String TOPIC_NAME = "0";

    private final Random generator = new Random();
    private final byte[] msg;
    private final int partitionNumber;

    public SingleTopicFixedLengthSupplier(int messageLength, int topicNumber, int partitionNumber) {
        this.partitionNumber = partitionNumber;
        this.msg = Utils.generateWithLength(messageLength);
    }

    @Override
    public ProducerRecord<byte[], byte[]> get() {
        int partitionId = generator.nextInt(partitionNumber);
        return new ProducerRecord<>(String.valueOf(TOPIC_NAME), partitionId, null, msg);
    }

    @Override
    public String toString() {
        return String.format("[%s] messageLength = %d, topicName = %d, partitionNumber = %d",
                MultipleTopicFixedLenghtSupplier.class.getSimpleName(), msg.length, TOPIC_NAME, partitionNumber);
    }
}
