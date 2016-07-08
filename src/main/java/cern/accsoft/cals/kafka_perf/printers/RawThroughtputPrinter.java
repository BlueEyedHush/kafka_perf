package cern.accsoft.cals.kafka_perf.printers;

import java.util.function.Consumer;

public class RawThroughtputPrinter implements Consumer<Double> {
    @Override
    public void accept(Double aDouble) {
        System.out.println(aDouble);
    }
}
