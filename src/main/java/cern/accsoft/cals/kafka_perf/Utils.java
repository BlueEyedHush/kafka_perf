package cern.accsoft.cals.kafka_perf;

public class Utils {
    public static String generateWithLength(int length) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < length; i++) {
            builder.append("A");
        }

        return builder.toString();
    }
}
