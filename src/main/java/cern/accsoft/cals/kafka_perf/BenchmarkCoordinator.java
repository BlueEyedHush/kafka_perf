package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.reporters.FileReporter;

import java.util.List;

public class BenchmarkCoordinator implements Runnable {
    private final List<BenchmarkingService> service;
    private final FileReporter reporter;

    public BenchmarkCoordinator(List<BenchmarkingService> service, FileReporter reporter) {
        this.service = service;
        this.reporter = reporter;
    }

    @Override
    public void run() {

    }
}
