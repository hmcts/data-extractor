package uk.gov.hmcts.reform.dataextractor.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import uk.gov.hmcts.reform.dataextractor.Factory;
import uk.gov.hmcts.reform.dataextractor.QueryExecutor;
import uk.gov.hmcts.reform.dataextractor.config.ExtractionFilters;
import uk.gov.hmcts.reform.dataextractor.exception.ExtractorException;
import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@SuppressWarnings("PMD.CloseResource") //Resource closed with QueryExecutor
public class CaseDataServiceImpl implements CaseDataService {

    private static final String FIRST_CREATED_QUERY = "select CE.created_date from case_event CE "
        + "where case_type_id = '%s' order by CE.created_date asc limit 1;";

    private static final String GET_CASE_TYPES = "select distinct jurisdiction, case_type_id from case_data "
        + "where";

    @Autowired
    private final Factory<String, QueryExecutor> queryExecutorFactory;

    @Autowired
    private final ExtractionFilters filters;

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

    @Override
    public void checkConnection() {
        try (QueryExecutor executor = queryExecutorFactory.provide("Select 1")) {
            executor.execute().next();
        } catch (SQLException e) {
            throw new ExtractorException(e);
        }
    }

    @Override
    public List<CaseDefinition> getCaseDefinitions() {
        String loadDataQuery;

        if (!CollectionUtils.isEmpty(filters.getIn())) {
            loadDataQuery = GET_CASE_TYPES + String.format(" jurisdiction in (%s)", String.join(", ", filters.getIn()));
        } else {
            loadDataQuery = GET_CASE_TYPES + String.format(" jurisdiction not in (%s)", String.join(", ", filters.getOut()));
        }

        try (QueryExecutor executor = queryExecutorFactory.provide(loadDataQuery)) {
            ResultSet resultSet = executor.execute();
            List<CaseDefinition> caseDefinitions = new ArrayList<>();
            while (resultSet.next()) {
                caseDefinitions.add(loadData(resultSet));
            }
            return caseDefinitions;
        } catch (SQLException e) {
            throw new ExtractorException(e);
        }
    }

    private CaseDefinition loadData(ResultSet resultSet) throws SQLException {
        String jurisdiction = resultSet.getString("jurisdiction");
        String caseType = resultSet.getString("case_type_id");
        return new CaseDefinition(jurisdiction, caseType);
    }
}
