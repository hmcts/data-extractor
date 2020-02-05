package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.io.OutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static uk.gov.hmcts.reform.dataextractor.service.ContainerConstants.UPDATE_DATE_METADATA;

@Service
@Primary
@Slf4j
public class BlobServiceImpl implements OutputStreamProvider {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final String clientId;
    private final String connectionString;
    private final String storageAccountName;
    private final BlobServiceClientFactory blobServiceClientFactory;

    @Autowired
    public BlobServiceImpl(@Value("${azure.managed-identity.client-id:}") String clientId,
                           @Value("${etl.connection-string:}") String connectionString,
                           @Value("${etl.account:}") String storageAccountName,
                           BlobServiceClientFactory blobServiceClientFactory) {
        this.clientId = clientId;
        this.blobServiceClientFactory = blobServiceClientFactory;
        this.connectionString = connectionString;
        this.storageAccountName = storageAccountName;
    }

    public LocalDate getContainerLastUpdated(String containerName) {
        BlobServiceClient blobServiceClient = getConnection();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (containerClient.exists()) {
            if (containerClient.getProperties().getMetadata() != null
                && containerClient.getProperties().getMetadata().get(UPDATE_DATE_METADATA) != null) {
                String updated = containerClient.getProperties().getMetadata().get(UPDATE_DATE_METADATA);
                return LocalDate.parse(updated, DATE_TIME_FORMATTER);
            }
            return null;
        } else {
            containerClient.create();
            return null;
        }
    }

    public void setLastUpdated(String containerName) {
        BlobContainerClient containerClient = getContainerClient(containerName);
        containerClient.setMetadata(Map.of(UPDATE_DATE_METADATA, DATE_TIME_FORMATTER.format(LocalDate.now())));
    }

    public OutputStream getOutputStream(String containerName, String fileName, Output outputType) {
        BlobContainerClient client = getContainerClient(containerName);
        BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(outputType.getApplicationContent());
        return client.getBlobClient(fileName).getBlockBlobClient()
            .getBlobOutputStream(null, headers, null, null, null);
    }

    public OutputStream getOutputStream(ExtractionData extractionData) {
        BlobContainerClient client = getContainerClient(extractionData.getContainer());
        BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(extractionData.getType().getApplicationContent());
        return client.getBlobClient(extractionData.getFileName()).getBlockBlobClient()
            .getBlobOutputStream(null, headers, null, null, null);
    }

    BlobContainerClient getContainerClient(String containerName) {
        BlobServiceClient blobServiceClient = getConnection();
        return blobServiceClient.getBlobContainerClient(containerName);
    }

    BlobServiceClient getConnection() {

        if (StringUtils.isBlank(clientId)) {
            return blobServiceClientFactory.getBlobClientWithConnectionString(connectionString);
        } else {
            return blobServiceClientFactory.getBlobClientWithManagedIdentity(clientId, storageAccountName);
        }
    }
}
