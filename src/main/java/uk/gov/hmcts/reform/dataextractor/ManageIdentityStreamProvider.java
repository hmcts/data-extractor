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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

@Component
@Primary
@ConditionalOnProperty(value = "etl.msi-client-id")
public class ManageIdentityStreamProvider implements OutputStreamProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManageIdentityStreamProvider.class);
    private static final String STORAGE_RESOURCE = "https://storage.azure.com/";
    private static final String CONNECTION_URI_TPL = "https://%s.blob.core.windows.net";

    private final String clientId;
    private final String accountName;

    public ManageIdentityStreamProvider(@Value("${etl.msi-client-id}") String clientId,
                                        @Value("${etl.account}") String accountName) {
        this.clientId = clientId;
        this.accountName = accountName;
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

    protected CloudBlobClient getClient() throws URISyntaxException {
        URI connectionUri = new URI(String.format(CONNECTION_URI_TPL, accountName));

        StorageCredentials storageCredentials = getCredentials();
        return new CloudBlobClient(connectionUri, storageCredentials);
    }

    public OutputStream getOutputStream(String containerName, String fileName, DataExtractorApplication.Output outputType) {
        try {
            CloudBlobClient client = getClient();

            CloudBlobContainer container = client.getContainerReference(containerName);

            if (!container.exists()) {
                container.create();
            }
            CloudBlockBlob blob = container.getBlockBlobReference(fileName);
            blob.getProperties().setContentType(outputType.getApplicationContent());
            return blob.openOutputStream();
        } catch (URISyntaxException | StorageException e) {
            throw new WriterException(e);
        }
    }

}
