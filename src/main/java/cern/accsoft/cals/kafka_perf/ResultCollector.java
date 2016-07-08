package cern.accsoft.cals.kafka_perf;

public interface ResultCollector {
    void beforeSeries();
    void afterSeries();
}
