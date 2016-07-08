package cern.accsoft.cals.kafka_perf;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public interface Reportee {
    Map<Integer, BlockingQueue<Long>> getResults();
}
