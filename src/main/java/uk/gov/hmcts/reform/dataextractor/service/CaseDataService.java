package uk.gov.hmcts.reform.dataextractor.service;

import uk.gov.hmcts.reform.dataextractor.model.CaseDefinition;
import uk.gov.hmcts.reform.dataextractor.model.ExtractionWindow;

import java.time.LocalDate;
import java.util.List;

public interface CaseDataService {

    LocalDate getFirstEventDate(String caseType);

    void checkConnection();

    List<CaseDefinition> getCaseDefinitions();

    ExtractionWindow getDates(String caseType);

    long getCaseTypeRows(String caseType);

    int calculateExtractionWindow(String caseType);
}
