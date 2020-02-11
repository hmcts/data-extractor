package uk.gov.hmcts.reform.dataextractor.service;

import java.time.LocalDate;

public interface CaseDataService {

    LocalDate getFirstEventDate(String caseType);

}
