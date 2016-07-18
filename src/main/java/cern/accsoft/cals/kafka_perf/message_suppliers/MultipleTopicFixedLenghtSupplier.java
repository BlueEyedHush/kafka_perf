package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class MultipleTopicFixedLenghtSupplier implements MessageSupplier {

    private final Random generator = new Random();
    private final byte[] msg;
    private final int topicNumber;
    private final int partitionNumber;

    public MultipleTopicFixedLenghtSupplier(int messageLength, int topicNumber, int partitionNumber) {
        this.partitionNumber = partitionNumber;
        this.msg = Utils.generateWithLength(messageLength);
        this.topicNumber = topicNumber;
    }

    @Override
    public ProducerRecord<byte[], byte[]> get() {
        int topicId = generator.nextInt(topicNumber);
        int partitionId = generator.nextInt(partitionNumber);
        return new ProducerRecord<>(String.valueOf(topicId), partitionId, null, msg);
    }

    @Override
    public String toString() {
        return String.format("[%s] messageLength = %d, topicNumber = %d, partitionNumber = %d",
                MultipleTopicFixedLenghtSupplier.class.getSimpleName(), msg.length, topicNumber, partitionNumber);
    }
}
