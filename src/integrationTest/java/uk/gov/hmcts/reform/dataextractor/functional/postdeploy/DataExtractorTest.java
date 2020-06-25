package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.DbConfig;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.ContainerConstants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.BLOB_PREFIX;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.BREAK_LINE;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.CASE_TYPE;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.DATA_DAYS;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.DATE_TIME_FORMATTER;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.OUTPUT_TYPE;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.TEST_CONTAINER;

@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application_e2e.properties")
@SpringBootTest(classes = TestApplicationConfiguration.class)
@Slf4j
public class DataExtractorTest {


    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private BlobReader blobReader;

    @Value("${container.name}")
    public String testContainerName;

    private static final  String SQL_QUERY_BY_CASETYPE = "SELECT count(*) \n"
        + "FROM case_event CE\n"
        + "WHERE CE.case_type_id = '%s'\n"
        + "AND created_date >= (current_date-%s + time '00:00')\n"
        + "AND created_date < (current_date + time '00:00')";


    @Test
    @SuppressWarnings("PMD.CloseResource")
    public void checkBlobAreCreated() throws SQLException {
        String expectedFileName = getFileName(BLOB_PREFIX, OUTPUT_TYPE);

        BlobClient blobClient = blobReader.getBlobClient(testContainerName.toLowerCase(Locale.UK), expectedFileName);
        assertTrue(blobClient.exists(), "Blob missing");

        String fileContent = blobReader.readFile(TEST_CONTAINER, expectedFileName);

        String sqlQuery = getQueryByCaseType(CASE_TYPE);
        final int fileLines = getFileLinesCount(fileContent);
        try (QueryExecutor queryExecutor = new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery)) {
            ResultSet resultSet = queryExecutor.execute();
            if (resultSet.next()) {
                int rows = resultSet.getInt(1);
                assertEquals(fileLines, rows, "Number de lines expected");
            } else {
                fail("Result not found");
            }
        }

        blobClient.delete();
        BlobContainerClient containerClient = blobReader.getBlobServiceClient().getBlobContainerClient(testContainerName.toLowerCase(Locale.UK));
        containerClient.setMetadata(Map.of(ContainerConstants.UPDATE_DATE_METADATA, StringUtils.EMPTY));
    }

    private int getFileLinesCount(String fileContent) {
        return StringUtils.split(fileContent, BREAK_LINE).length;
    }

    private String getFileName(String prefix, Output extension) {
        return String.format("%s-%s.%s", prefix, DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))),
            extension.getExtension());
    }

    private String getQueryByCaseType(String caseType) {
        return String.format(SQL_QUERY_BY_CASETYPE, caseType, DATA_DAYS);
    }
}
