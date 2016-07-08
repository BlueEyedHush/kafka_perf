package cern.accsoft.cals.kafka_perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger("main");

    public static void main( String[] args ) {
        LOGGER.info("Application started");
        new App().start();
        LOGGER.info("Application terminated");
    }

    private void start() {

    }
}
