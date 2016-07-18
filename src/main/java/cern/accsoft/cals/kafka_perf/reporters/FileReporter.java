package cern.accsoft.cals.kafka_perf.reporters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * This reporter is run after all of the probes finish
 */
public class FileReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileReporter.class);
    private static final OpenOption[] LOG_FILE_OPEN_OPTIONS = {StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING};

    private final Path outFile;

    public FileReporter(Path outFile) {
        this.outFile = outFile;
    }

    public void report(List<Long> values) {
        try(BufferedWriter writer = Files.newBufferedWriter(outFile, StandardCharsets.UTF_8, LOG_FILE_OPEN_OPTIONS)) {
            writeValuesToWriter(writer, values);
        } catch (IOException e) {
            LOGGER.error("IOException while trying to save data to log file. Dumping it to STDOUT", e);

            try {
                writeValuesToWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), values);
            } catch (IOException e1) {
                LOGGER.error("Second IOException while trying to write to STDOUT. Aborting.");
            }
        }
    }

    private void writeValuesToWriter(Writer writer, List<Long> values) throws IOException {
        for(Long value: values) {
            writer.write(value.toString());
            writer.write('\n');
        }

        writer.flush();
    }
}
