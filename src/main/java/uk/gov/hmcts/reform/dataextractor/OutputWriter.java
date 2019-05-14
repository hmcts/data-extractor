package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.credentials.MSICredentials;
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


public class OutputWriter implements AutoCloseable {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final String CONNECTION_URI_TPL = "https://%s.blob.core.windows.net";
    
    private final String accountName;
    private final String container;
    private final String filePrefix;
    private final DataExtractorApplication.Output outputType;
    private OutputStream outputStream;

    public OutputWriter(
        String accountName, String container, String filePrefix, DataExtractorApplication.Output outputType
    ) {
        this.accountName = accountName;
        this.container = container;
        this.filePrefix = filePrefix;
        this.outputType = outputType;
    }

    private StorageCredentials getCredentials() {
        MSICredentials credsProvider = new MSICredentials(); //MSICredentials.getMSICredentials();
        try {
            String accessToken = credsProvider.getToken(null);
            return new StorageCredentialsToken(accountName, accessToken);
        } catch (IOException e) {
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
            container = client.getContainerReference(this.container);
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
