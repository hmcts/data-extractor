package uk.gov.hmcts.reform.dataextractor.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import uk.gov.hmcts.reform.dataextractor.ExtractionComponent;
import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.TestApplicationConfiguration;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionData;
import uk.gov.hmcts.reform.dataextractor.config.Extractions;
import uk.gov.hmcts.reform.dataextractor.model.Output;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

@ExtendWith(SpringExtension.class)
@ActiveProfiles(profiles = "localtest")
@SpringBootTest(classes = TestApplicationConfiguration.class)
public class BlobServiceFTest {

    private static final String CONTAINER_NAME = "test-container";
    @Autowired
    private BlobServiceImpl blobService;

    @Autowired
    private ExtractionComponent dataExtractorApplication;

    @Autowired
    private Factory<String, QueryExecutor> queryExecutorFactory;

    @Autowired
    private Extractions extractions;

    @Autowired
    private Factory<Output, Extractor> extractorFactory;

    //    @Test
    public void test() {
        dataExtractorApplication.execute();
    }

    @Test
    public void test2() throws SQLException {
        LocalDate date = blobService.getContainerLastUpdated(CONTAINER_NAME);
        QueryExecutor executor = getQueryExecutor();
        ResultSet resultSet = executor.execute();
        if (resultSet.isBeforeFirst()) {
            ExtractionData extractionData = ExtractionData.builder()
                .type(Output.JSON)
                .caseType("div")
                .prefix("test")
                .container(CONTAINER_NAME)
                .build();
            OutputStream writer = blobService.getOutputStream(extractionData);

            Extractor extractor = extractorFactory.provide(Output.JSON);
            extractor.apply(resultSet, writer);
        }
    }

    QueryExecutor getQueryExecutor() {
        return queryExecutorFactory.provide("select * from qrtz_triggers");
    }
}
