package uk.gov.hmcts.reform.dataextractor.functional.predeploy;

import com.azure.storage.blob.BlobContainerClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import uk.gov.hmcts.reform.dataextractor.functional.postdeploy.BlobReader;
import uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestApplicationConfiguration;
import uk.gov.hmcts.reform.dataextractor.service.ContainerConstants;
import uk.gov.hmcts.reform.dataextractor.utils.DateTimeUtils;

import java.time.LocalDate;
import java.util.Locale;
import java.util.Map;

import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.DATA_DAYS;

@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application_e2e.properties")
@SpringBootTest(classes = TestApplicationConfiguration.class)
@Slf4j
public class DataExtractorPreDeployTest {

    @Autowired
    private BlobReader blobReader;

    @Value("${container.name}")
    public String testContainerName;

    @Test
    public void prepareContainer() {
        final String containerName = testContainerName.toLowerCase(Locale.UK);
        try {
            log.info(containerName);
            BlobContainerClient containerClient = blobReader.getBlobServiceClient().getBlobContainerClient(containerName);
            if (!containerClient.exists()) {
                containerClient.create();
            }

            String metadataValue = DateTimeUtils.dateToString(LocalDate.now().minusDays(DATA_DAYS));
            containerClient.setMetadata(Map.of(ContainerConstants.UPDATE_DATE_METADATA,
                DateTimeUtils.dateToString(LocalDate.now().minusDays(DATA_DAYS))));
            log.info("Set metadata value as {} for container {}", metadataValue, containerName);
        } catch (Exception e) {
            log.error("Error preparing container", e);
            Assert.fail(containerName + e.getMessage());
        }

    }
}
