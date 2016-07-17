package cern.accsoft.cals.kafka_perf;

import java.util.Random;

public class Utils {
    private static final Random generator = new Random();

    public static byte[] generateWithLength(int length) {
        byte[] arr = new byte[length];
        generator.nextBytes(arr);
        return arr;
    }
}
