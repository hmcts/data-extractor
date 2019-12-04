package uk.gov.hmcts.reform.dataextractor.functional.postdeploy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.DbConfig;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application_e2e.properties")
@SpringBootTest
public class DataExtractorTest {

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private BlobReader blobReader;

    @Test
    public void test() {
        String sqlQuery = "SELECT count(*) \n"
            + "FROM case_event CE\n"
            + "WHERE CE.case_type_id = 'GrantOfRepresentation'\n"
            + "AND created_date >= (current_date-1 + time '00:00')\n"
            + "AND created_date < (current_date + time '00:00')";

        QueryExecutor queryExecutor = new QueryExecutor(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword(), sqlQuery);
        try {
            ResultSet resultSet = queryExecutor.execute();
            resultSet.next();
            int rows = resultSet.getInt(1);
            blobReader.readFile("divorce", "DIV-20191129.jsonl");
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
}
