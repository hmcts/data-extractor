package uk.gov.hmcts.reform.dataextractor.service;

import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;

import java.time.LocalDate;
import java.util.List;

public interface CaseDataService {

    LocalDate getFirstEventDate(String caseType);

    void checkConnection();

    List<CaseDefinition> getCaseDefinitions();
}
