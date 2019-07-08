package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.msiAuthTokenProvider.AzureMSICredentialException;
import com.microsoft.azure.msiAuthTokenProvider.MSICredentials;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;


public class BlobOutputWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobOutputWriter.class);

    private static final String STORAGE_RESOURCE = "https://storage.azure.com/";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final String CONNECTION_URI_TPL = "https://%s.blob.core.windows.net";

    private static final int OUTPUT_BUFFER_SIZE = 100_000_000;

    private final String clientId;
    private final String accountName;
    private final String containerName;
    private final String filePrefix;
    private final DataExtractorApplication.Output outputType;
    private OutputStream outputStream;

    public BlobOutputWriter(
            String clientId, String accountName, String containerName,
            String filePrefix, DataExtractorApplication.Output outputType
    ) {
        this.clientId = clientId;
        this.accountName = accountName;
        this.containerName = containerName;
        this.filePrefix = filePrefix;
        this.outputType = outputType;
    }

    protected StorageCredentials getCredentials() {
        MSICredentials credsProvider = MSICredentials.getMSICredentials();
        credsProvider.updateClientId(clientId);
        try {
            String accessToken = credsProvider.getToken(STORAGE_RESOURCE).accessToken();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got access token: {}", accessToken != null ? accessToken.substring(0, 5) + "..." : "null");
            }
            return new StorageCredentialsToken(accountName, accessToken);
        } catch (IOException | AzureMSICredentialException e) {
            throw new WriterException(e);
        }
    }

    protected CloudBlobClient getClient() {
        URI connectionUri = null;
        try {
            connectionUri = new URI(String.format(CONNECTION_URI_TPL, accountName));
        } catch (URISyntaxException e) {
            throw new WriterException(e);
        }
        StorageCredentials storageCredentials = getCredentials();
        return new CloudBlobClient(connectionUri, storageCredentials);
    }

    public OutputStream outputStream() {
        if (outputStream != null) {
            return outputStream;
        }

        CloudBlobClient client = getClient();
        return outputStream(client);
    }

    OutputStream outputStream(CloudBlobClient client) {
        if (outputStream != null) {
            return outputStream;
        }

        String fileName = new StringBuilder()
                .append(filePrefix)
                .append("-")
                .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))))
                .append(".")
                .append(outputType.getExtension())
                .toString();
        CloudBlobContainer container = null;
        try {
            container = client.getContainerReference(this.containerName);
            CloudBlockBlob blob = container.getBlockBlobReference(fileName);
            blob.getProperties().setContentType(outputType.getApplicationContent());
            outputStream = new BufferedOutputStream(blob.openOutputStream(), OUTPUT_BUFFER_SIZE);
        } catch (URISyntaxException | StorageException e) {
            throw new WriterException(e);
        }
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
            LOGGER.warn("Could not close stream. Root cause is: {}", e.getMessage());
        }
    }

}
