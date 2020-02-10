package uk.gov.hmcts.reform.dataextractor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.DbTest;
import uk.gov.hmcts.reform.dataextractor.TestApplicationConfiguration;
import uk.gov.hmcts.reform.dataextractor.config.DbConfig;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.impl.CaseDataServiceImpl;
import uk.gov.hmcts.reform.dataextractor.utils.DateTimeUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@ExtendWith(SpringExtension.class)
@ActiveProfiles(profiles = "test")
@SpringBootTest(classes = TestApplicationConfiguration.class)
public class CaseDataServiceFTest extends DbTest {

    @Autowired
    private CaseDataServiceImpl caseDataService;

    @Autowired
    private DbConfig dbConfig;

    @BeforeEach
    public void setUp() {
        // Config created by test containers
        ReflectionTestUtils.setField(dbConfig, "url", jdbcUrl);
        ReflectionTestUtils.setField(dbConfig, "user", username);
        ReflectionTestUtils.setField(dbConfig, "password", password);
    }

    @Test
    public void givenValidCaseType_theReturnFirstRecordDate() {
        assertEquals(DateTimeUtils.stringToDate("2019-12-10"),
            caseDataService.getFirstEventDate("test"), "Expected first record date");
    }

    @Test
    public void givenCaseTypeWithoutData_thenThrowNoDataException() {
        assertThrows(ExtractorException.class, () ->
            caseDataService.getFirstEventDate("nonExistingCase"), "Expected Extractor exception");
    }
}
