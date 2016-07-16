package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class MultipleTopicFixedLenghtSupplier implements MessageSupplier {

    private final String msg;
    private final int topicNumber;
    private final Random generator = new Random();

    public MultipleTopicFixedLenghtSupplier(int messageLength, int topicNumber) {
        this.msg = Utils.generateWithLength(messageLength);
        this.topicNumber = topicNumber;
    }

    @Override
    public ProducerRecord<String, String> get() {
        int topicId = generator.nextInt(topicNumber);
        return new ProducerRecord<>(String.valueOf(topicId), msg);
    }
}
