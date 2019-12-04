package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import com.azure.storage.blob.BlobClient;
import org.junit.Ignore;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.gov.hmcts.reform.dataextractor.DataExtractorApplication;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.DbConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application_e2e.properties")
@SpringBootTest
public class DataExtractorTest {
    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private BlobReader blobReader;

    @Autowired
    private Connection dbConnection;
    private static final  String SQL_QUERY_BY_CASETYPE = "SELECT count(*) \n"
        + "FROM case_event CE\n"
        + "WHERE CE.case_type_id = '%s'\n"
        + "AND created_date >= (current_date-1 + time '00:00')\n"
        + "AND created_date < (current_date + time '00:00')";


    static Stream<Arguments> caseTypeInfoProvider() {
        return Stream.of(
            arguments("probate", "GrantOfRepresentation", DataExtractorApplication.Output.JSON_LINES, "PROB-GOR"),
            arguments("divorce", "divorce", DataExtractorApplication.Output.JSON_LINES, "DIV")
        );
    }

    @ParameterizedTest(name = "#{index} - Testing caseType: {1}")
    @MethodSource("caseTypeInfoProvider")
    @Ignore // Do not verify DB content
    public void checkByCaseType(String container, String caseType, DataExtractorApplication.Output type, String prefix) {
        String sqlQuery = getQueryByCaseType(caseType);
        String fileName = getFileName(prefix, type);
        QueryExecutor queryExecutor = new QueryExecutor(dbConnection, sqlQuery);
        try {
            ResultSet resultSet = queryExecutor.execute();
            resultSet.next();
            int rows = resultSet.getInt(1);
            String blobContent = blobReader.readFile(container, fileName);
            assertTrue(rows > 0, "Rows numbers " + rows);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                queryExecutor.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @ParameterizedTest(name = "#{index} - Testing caseType: {1}")
    @MethodSource("caseTypeInfoProvider")
    public void checkBlobAreCreated(String container, String caseType, DataExtractorApplication.Output type, String prefix) {
        String fileName = getFileName(prefix, type);

        BlobClient blobClient = blobReader.getBlobClient(container, fileName);
        assertTrue(blobClient.exists(), "Blob missing");
        assertTrue(blobClient.getProperties().getBlobSize() > 0, "Blob empty");
    }

    public String getFileName(String prefix, DataExtractorApplication.Output extension) {
        return String.format("%s-%s.%s", prefix, DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneId.from(UTC))),
            extension.getExtension());
    }

    private String getQueryByCaseType(String caseType) {
        return String.format(SQL_QUERY_BY_CASETYPE, caseType);
    }
}
