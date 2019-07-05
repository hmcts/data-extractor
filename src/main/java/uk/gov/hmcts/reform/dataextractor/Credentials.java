package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.azure.msiAuthTokenProvider.AzureMSICredentialException;
import com.microsoft.azure.msiAuthTokenProvider.MSICredentials;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class Credentials {

    private static final Logger LOGGER = LoggerFactory.getLogger(Credentials.class);

    private Credentials() {
    }

    public static StorageCredentials get(String clientId, String storageResource, String accountName) {
        MSICredentials credsProvider = MSICredentials.getMSICredentials();
        credsProvider.updateClientId(clientId);
        try {
            String accessToken = credsProvider.getToken(storageResource).accessToken();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got access token: {}", accessToken != null ? accessToken.substring(0, 5) + "..." : "null");
            }
            return new StorageCredentialsToken(accountName, accessToken);
        } catch (IOException | AzureMSICredentialException e) {
            throw new WriterException(e);
        }
    }
}
