package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties("extraction.filter.jurisdiction")
@Getter
public class ExtractionFilters {
    List<String> in = new ArrayList<>();
    List<String> out = new ArrayList<>();
}
