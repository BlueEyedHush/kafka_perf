package cern.accsoft.cals.kafka_perf;

public interface Probe {
    void beforeSeries();
    void afterSeries();
}
