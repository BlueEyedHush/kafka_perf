package cern.accsoft.cals.kafka_perf.message_suppliers;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Supplier;

/**
 * To have shorter types names
 */
public interface MessageSupplier extends Supplier<ProducerRecord<String, String>> {
}
