package uk.gov.hmcts.reform.dataextractor.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

@Component
public class CaseDataService {

    @Autowired
    private Factory<String, QueryExecutor> queryExecutorFactory;

    private static final String FIRST_CREATED_QUERY = "select CE.created_date from case_event CE "
        + "where case_type_id = '%s' order by CE.created_date asc limit 1;";


    public LocalDate getFirstEventDate(String caseType) {
            String query = String.format(FIRST_CREATED_QUERY, caseType);
            try (QueryExecutor executor = queryExecutorFactory.provide(query)) {
                ResultSet resultSet = executor.execute();
                if (resultSet.next()) {
                    Date date =  resultSet.getDate(1);
                    return date.toLocalDate();
                } else {
                    throw new ExtractorException("Empty");
                }

            } catch (SQLException e) {
                throw new ExtractorException(e);
            }
    }

}
