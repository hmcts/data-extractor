package uk.gov.hmcts.reform.dataextractor;

import com.azure.spring.autoconfigure.storage.StorageAutoConfiguration;
import com.microsoft.applicationinsights.autoconfigure.ApplicationInsightsWebMvcAutoConfiguration;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

@Configuration
@ComponentScan(
        basePackages = {"uk.gov.hmcts.reform"},
        excludeFilters = {
            @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = ApplicationRunner.class)
        }
    )
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class, StorageAutoConfiguration.class,
    ApplicationInsightsWebMvcAutoConfiguration.class })
public class TestApplicationConfiguration {
}
