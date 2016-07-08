package cern.accsoft.cals.kafka_perf.printers;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.function.Consumer;

public class PrettyThroughputPrinter implements Consumer<Double> {
    private final DecimalFormat formatter;

    public PrettyThroughputPrinter() {
        formatter = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
        DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();

        symbols.setGroupingSeparator(' ');
        formatter.setDecimalFormatSymbols(symbols);
    }

    @Override
    public void accept(Double throughputValue) {
        System.out.println(formatter.format(throughputValue)  + " B/s");
    }
}
