package uk.gov.hmcts.reform.dataextractor;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;


public class BlobOutputWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobOutputWriter.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final int OUTPUT_BUFFER_SIZE = 100_000_000;

    private final String clientId;
    private final String accountName;
    private final String containerName;
    private final String filePrefix;
    private final String connectionString;
    private final DataExtractorApplication.Output outputType;

    private OutputStream outputStream;


    public BlobOutputWriter(
            String clientId, String accountName, String containerName,
            String filePrefix, DataExtractorApplication.Output outputType,
            String connectionString
    ) {
        this.clientId = clientId;
        this.accountName = accountName;
        this.containerName = containerName;
        this.filePrefix = filePrefix;
        this.outputType = outputType;
        this.connectionString = connectionString;
        String connectStr = "DefaultEndpointsProtocol=https;AccountName=midatastgsbox;AccountKey=9NjPuuZ8ol1qikuKNJTTlMQsJFAkxXsfoq2OW+HQMXq7TkaROcX2oXNAunE0z+93jPZGCeDm8CpWDrd0U/GvaQ==;EndpointSuffix=core.windows.net";
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
        } catch (IOException e) {
            // Blob storage client has already closed the stream. This exception cannot be
            // re-thrown as otherwise if this is run as a kubernetes job, it will keep being
            // restarted and the same file generated again and again.
            LOGGER.warn("Could not close stream.", e);
        }
    }

    protected OutputStreamProvider getOutputStreamProvider() {
        if (!Strings.isNullOrEmpty(connectionString)) {
            return new ApiKeyStreamProvider(connectionString);
        } else {
            return new ManageIdentityStreamProvider(clientId, accountName);
        }
    }

    private String fileName() {
        return  new StringBuilder()
            .append(filePrefix)
            .append("-")
            .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))))
            .append(".")
            .append(outputType.getExtension())
            .toString();
    }

}
