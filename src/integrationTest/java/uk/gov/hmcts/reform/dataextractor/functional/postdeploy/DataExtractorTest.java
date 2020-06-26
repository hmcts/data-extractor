package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.BLOB_PREFIX;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.CASE_TYPE;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.DATA_DAYS;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.DATE_TIME_FORMATTER;
import static uk.gov.hmcts.reform.dataextractor.functional.postdeploy.TestConstants.OUTPUT_TYPE;

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
    private String testContainerName;


    private static final  String SQL_QUERY_BY_CASETYPE = "SELECT count(*) \n"
        + "FROM case_event CE\n"
        + "WHERE CE.case_type_id = '%s'\n"
        + "AND created_date >= (current_date-%s + time '00:00')\n"
        + "AND created_date < (current_date + time '00:00')";


    @BeforeEach
    public void setUp() {
        testContainerName = testContainerName.toLowerCase(Locale.UK);
    }

    @Test
    @SuppressWarnings("PMD.CloseResource")
    public void checkBlobAreCreated() throws SQLException, IOException {
        String expectedFileName = getFileName(BLOB_PREFIX, OUTPUT_TYPE);

        BlobClient blobClient = blobReader.getBlobClient(testContainerName, expectedFileName);
        assertTrue(blobClient.exists(), "Blob missing");

        String sqlQuery = getQueryByCaseType(CASE_TYPE);
        long fileLines;
        try (BufferedReader blobBufferReader = blobReader.getBufferReader(testContainerName, expectedFileName)) {
            fileLines = blobBufferReader.lines().count();
        }

        try (QueryExecutor queryExecutor = new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery)) {
            ResultSet resultSet = queryExecutor.execute();
            if (resultSet.next()) {
                long rows = resultSet.getLong(1);
                assertEquals(fileLines, rows, "Number de lines expected");
            } else {
                fail("Result not found");
            }
        }

        blobClient.delete();
        BlobContainerClient containerClient = blobReader.getBlobServiceClient().getBlobContainerClient(testContainerName.toLowerCase(Locale.UK));
        if (containerClient.exists()) {
            containerClient.delete();
        }
    }

    private String getFileName(String prefix, Output extension) {
        return String.format("%s-%s.%s", prefix, DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))),
            extension.getExtension());
    }

    private String getQueryByCaseType(String caseType) {
        return String.format(SQL_QUERY_BY_CASETYPE, caseType, DATA_DAYS);
    }
}
