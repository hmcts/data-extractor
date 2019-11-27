package uk.gov.hmcts.reform.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;

@Component
public class BlobOutputWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobOutputWriter.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final int OUTPUT_BUFFER_SIZE = 100_000_000;

    private final String containerName;

    private final String filePrefix;

    private final  DataExtractorApplication.Output outputType;

    @Autowired
    private OutputStreamProvider getOutputStreamProvider;

    private OutputStream outputStream;

    public BlobOutputWriter(@Value("${etl.container}") String containerName,
                            @Value("${etl.file.prefix}")String filePrefix,
                            @Value("${etl.file.type}") DataExtractorApplication.Output outputType
    ) {
        this.containerName = containerName;
        this.filePrefix = filePrefix;
        this.outputType = outputType;
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
            LOGGER.warn("Could not close stream.", e);
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
