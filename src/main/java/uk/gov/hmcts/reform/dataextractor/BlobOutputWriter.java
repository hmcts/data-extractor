package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.msiAuthTokenProvider.AzureMSICredentialException;
import com.microsoft.azure.msiAuthTokenProvider.MSICredentials;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;


public class BlobOutputWriter implements AutoCloseable {

    private static final String STORAGE_RESOURCE = "https://storage.azure.com/";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final String CONNECTION_URI_TPL = "https://%s.blob.core.windows.net";

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

    private StorageCredentials getCredentials() {
        MSICredentials credsProvider = MSICredentials.getMSICredentials();
        credsProvider.updateClientId(clientId);
        try {
            String accessToken = credsProvider.getToken(STORAGE_RESOURCE).accessToken();
            return new StorageCredentialsToken(accountName, accessToken);
        } catch (IOException | AzureMSICredentialException e) {
            throw new WriterException(e);
        }
    }

    private CloudBlobClient getClient() {
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
                .append(outputType.getExtension())
                .toString();
        CloudBlobContainer container = null;
        try {
            container = client.getContainerReference(this.containerName);
            CloudBlockBlob blob = container.getBlockBlobReference(fileName);
            blob.getProperties().setContentType(outputType.getApplicationContent());
            outputStream = blob.openOutputStream();
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
            throw new WriterException(e);
        }
    }

}
