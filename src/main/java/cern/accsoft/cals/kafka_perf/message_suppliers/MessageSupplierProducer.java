package cern.accsoft.cals.kafka_perf.message_suppliers;

public interface MessageSupplierProducer {
    MessageSupplier create(int messageSize, int topicCount, int partitionsPerTopic);
}
