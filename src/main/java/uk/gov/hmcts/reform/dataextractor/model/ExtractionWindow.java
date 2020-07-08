package uk.gov.hmcts.reform.dataextractor.model;

import lombok.Value;

@Value
public class ExtractionWindow {
    long start;
    long end;
}
