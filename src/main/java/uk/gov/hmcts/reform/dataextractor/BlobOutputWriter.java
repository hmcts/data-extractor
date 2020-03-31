package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;

import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;

import java.io.OutputStream;

@Slf4j
public class BlobOutputWriter implements AutoCloseable {

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
        outputStream = getOutputStreamProvider().getOutputStream(containerName, fileName, outputType);
        return outputStream;
    }

    public OutputStream outputStream(String namePrefix) {
        if (outputStream != null) {
            return outputStream;
        }
        outputStream =  getOutputStreamProvider().getOutputStream(containerName, namePrefix, outputType);
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
