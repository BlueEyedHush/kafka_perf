package cern.accsoft.cals.kafka_perf.message_suppliers;

import cern.accsoft.cals.kafka_perf.Utils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Supplier;

public class SingleTopicFixedLengthSupplier implements Supplier<ProducerRecord<String, String>> {

    private final String msg;

    public SingleTopicFixedLengthSupplier(int messageLength) {
        this.msg = Utils.generateWithLength(messageLength);
    }

    @Override
    public ProducerRecord<String, String> get() {
        return new ProducerRecord<String, String>("test_topic", msg);
    }
}
