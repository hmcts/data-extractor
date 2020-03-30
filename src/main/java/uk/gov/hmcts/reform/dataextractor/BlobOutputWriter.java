package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

@Slf4j
public class BlobOutputWriter implements AutoCloseable {

    static final int OUTPUT_BUFFER_SIZE = 100_000_000;

    private final String containerName;

    private final String fileName;

    private final Output outputType;

    private final OutputStreamProvider getOutputStreamProvider;

    private OutputStream outputStream;

    public BlobOutputWriter(String containerName,
                            String fileName,
                            Output outputType,
                            OutputStreamProvider outputStreamProvider) {
        this.containerName = containerName;
        this.fileName = fileName;
        this.outputType = outputType;
        this.getOutputStreamProvider = outputStreamProvider;
    }

    public OutputStream outputStream() {
        if (outputStream != null) {
            return outputStream;
        }
        outputStream = new BufferedOutputStream(getOutputStreamProvider().getOutputStream(containerName, fileName, outputType), OUTPUT_BUFFER_SIZE);
        return outputStream;
    }

    public OutputStream outputStream(String namePrefix) {
        if (outputStream != null) {
            return outputStream;
        }
        outputStream = new BufferedOutputStream(getOutputStreamProvider().getOutputStream(containerName,
            namePrefix, outputType), OUTPUT_BUFFER_SIZE);
        return outputStream;
    }

    public void close() {
        try {
            if (outputStream != null) {
                outputStream.close();
                outputStream = null;
                log.info("Closed output stream form buffer name {}", this.fileName);
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
}
