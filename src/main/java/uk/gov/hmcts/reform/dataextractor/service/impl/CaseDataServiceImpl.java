package uk.gov.hmcts.reform.dataextractor.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

@Component
@RequiredArgsConstructor
public class CaseDataServiceImpl implements CaseDataService {

    @Autowired
    private final Factory<String, QueryExecutor> queryExecutorFactory;

    private static final String FIRST_CREATED_QUERY = "select CE.created_date from case_event CE "
        + "where case_type_id = '%s' order by CE.created_date asc limit 1;";


    @SuppressWarnings("PMD.CloseResource") //Resource closed with QueryExecutor
    public LocalDate getFirstEventDate(String caseType) {
        String query = String.format(FIRST_CREATED_QUERY, caseType);
        try (QueryExecutor executor = queryExecutorFactory.provide(query)) {
            ResultSet resultSet = executor.execute();
            if (resultSet.next()) {
                Date date = resultSet.getDate(1);
                return date.toLocalDate();
            } else {
                throw new ExtractorException("Case type without data");
            }

        } catch (SQLException e) {
            throw new ExtractorException(e);
        }
    }

}
