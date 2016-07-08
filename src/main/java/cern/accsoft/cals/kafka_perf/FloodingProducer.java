package cern.accsoft.cals.kafka_perf;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

public class FloodingProducer implements Runnable {
    @Override
    public void run() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Configuration.KAFKA_CONFIGURATION);
    }
}
