package uk.gov.hmcts.reform.dataextractor.model;

import lombok.Value;

@Value
public class CaseDefinition {
    private String jurisdiction;
    private String caseType;
}
