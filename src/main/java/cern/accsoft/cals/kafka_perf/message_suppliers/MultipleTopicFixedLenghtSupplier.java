package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Supplier;

public class MultipleTopicFixedLenghtSupplier implements Supplier<ProducerRecord<String, String>> {

    private final String msg;
    private final int topicNumber;

    private int nextTopicId = 0;

    public MultipleTopicFixedLenghtSupplier(int messageLength, int topicNumber) {
        this.msg = Utils.generateWithLength(messageLength);
        this.topicNumber = topicNumber;
    }

    @Override
    public ProducerRecord<String, String> get() {
        if(nextTopicId >= topicNumber) {
            nextTopicId = 0;
        }

        return new ProducerRecord<String, String>(String.valueOf(nextTopicId++), msg);
    }
}
