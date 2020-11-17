package uk.gov.hmcts.reform.dataextractor.service.impl;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ParallelTransferOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.BlobOutputValidator;
import uk.gov.hmcts.reform.dataextractor.service.OutputStreamProvider;
import uk.gov.hmcts.reform.mi.micore.factory.BlobServiceClientFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

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
    private final Factory<Output, BlobOutputValidator> blobOutputValidatorFactory;

    @Autowired
    public BlobServiceImpl(@Value("${etl.msi-client-id:}") String clientId,
                           @Value("${etl.connection-string:}") String connectionString,
                           @Value("${etl.account:}") String storageAccountName,
                           BlobServiceClientFactory blobServiceClientFactory,
                           Factory<Output, BlobOutputValidator>  blobOutputValidatorFactory) {
        this.clientId = clientId;
        this.blobServiceClientFactory = blobServiceClientFactory;
        this.connectionString = connectionString;
        this.storageAccountName = storageAccountName;
        this.blobOutputValidatorFactory = blobOutputValidatorFactory;
    }

    public LocalDate getContainerLastUpdated(String containerName, boolean initialise) {
        BlobServiceClient blobServiceClient = getConnection();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (containerClient.exists()) {
            if (containerClient.getProperties().getMetadata() != null
                && containerClient.getProperties().getMetadata().get(UPDATE_DATE_METADATA) != null) {
                String updated = containerClient.getProperties().getMetadata().get(UPDATE_DATE_METADATA);
                return LocalDate.parse(updated, DATE_TIME_FORMATTER);
            }
        } else if (initialise) {
            containerClient.create();
            log.info("Container {} created", containerName);
        }

        return null;
    }

    public void setLastUpdated(String containerName, LocalDate lastUpdateDate) {
        BlobContainerClient containerClient = getContainerClient(containerName);
        containerClient.setMetadata(Map.of(UPDATE_DATE_METADATA, DATE_TIME_FORMATTER.format(lastUpdateDate)));
    }

    public OutputStream getOutputStream(String containerName, String fileName, Output outputType) {
        BlobContainerClient client = getContainerClient(containerName);
        BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(outputType.getApplicationContent());
        Integer blockSize = 204800;
        int bufferNumbers = 10;

        ParallelTransferOptions transferOptions = new ParallelTransferOptions(blockSize, bufferNumbers, new BlobProgressReceiver());

        log.debug("Returning blobstream  with name {}", fileName);
        return client.getBlobClient(fileName).getBlockBlobClient()
            .getBlobOutputStream(transferOptions, headers, null, null, null);
    }


    public boolean validateBlob(String containerName, String fileName, Output outputType) {
        BlobOutputValidator blobValidator = blobOutputValidatorFactory.provide(outputType);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.getInputStream(containerName, fileName)))) {
            Optional<String> anyInvalid = reader.lines()
                .filter(blobValidator::isNotValid)
                .findFirst();
            return !anyInvalid.isPresent();
        } catch (Exception e) {
            log.error("unable to read file {} from container", fileName, containerName, e);
            return false;
        }
    }

    public void deleteBlob(String containerName, String fileName) {
        getContainerClient(containerName).getBlobClient(fileName).delete();
    }

    public PagedIterable<BlobContainerItem> listContainers() {
        return getConnection().listBlobContainers();
    }

    public PagedIterable<BlobItem> listContainerBlobs(String containerName) {
        return getContainerClient(containerName).listBlobs();
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

    InputStream getInputStream(String containerName, String fileName) {
        BlobContainerClient client = getContainerClient(containerName);
        log.debug("Returning blobstream  with name {}", fileName);
        return client.getBlobClient(fileName).openInputStream();
    }
}
