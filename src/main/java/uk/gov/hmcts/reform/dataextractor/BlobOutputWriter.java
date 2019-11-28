package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;

@Slf4j
public class BlobOutputWriter implements AutoCloseable {

    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    static final int OUTPUT_BUFFER_SIZE = 100_000_000;

    private final String containerName;

    private final String filePrefix;

    private final  DataExtractorApplication.Output outputType;

    private final OutputStreamProvider getOutputStreamProvider;

    private OutputStream outputStream;

    public BlobOutputWriter(String containerName, String filePrefix, DataExtractorApplication.Output outputType,
                            OutputStreamProvider outputStreamProvider) {
        this.containerName = containerName;
        this.filePrefix = filePrefix;
        this.outputType = outputType;
        this.getOutputStreamProvider = outputStreamProvider;
    }

    public OutputStream outputStream() {
        if (outputStream != null) {
            return outputStream;
        }
        outputStream = new BufferedOutputStream(getOutputStreamProvider().getOutputStream(containerName, fileName(), outputType), OUTPUT_BUFFER_SIZE);
        return outputStream;
    }

    public void close() {
        try {
            if (outputStream != null) {
                outputStream.flush();
                outputStream.close();
            }
        } catch (Exception e) {
            // Blob storage client has already closed the stream. This exception cannot be
            // re-thrown as otherwise if this is run as a kubernetes job, it will keep being
            // restarted and the same file generated again and again.
            log.warn("Could not close stream.", e);
        }
    }

    protected OutputStreamProvider getOutputStreamProvider() {
        return getOutputStreamProvider;
    }

    private String fileName() {
        return new StringBuilder()
            .append(filePrefix)
            .append("-")
            .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))))
            .append(".")
            .append(outputType.getExtension())
            .toString();
    }

}
