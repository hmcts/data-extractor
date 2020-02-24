package uk.gov.hmcts.reform.dataextractor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import uk.gov.hmcts.reform.mi.micore.component.HealthCheck;
import uk.gov.hmcts.reform.mi.micore.exception.ServiceNotAvailableException;

@Slf4j
@SpringBootApplication(scanBasePackages = "uk.gov.hmcts.reform", exclude = {DataSourceAutoConfiguration.class})
public class DataExtractorApplication implements ApplicationRunner {

    @Autowired
    private ExtractionComponent extractionComponent;

    @Autowired
    private HealthCheck healthCheck;

    @Value("${smoke.test.enabled:false}")
    private boolean smokeTest;

    @Override
    public void run(ApplicationArguments args) throws ServiceNotAvailableException {
        if (smokeTest) {
            healthCheck.check();
        } else {
            extractionComponent.execute();
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DataExtractorApplication.class);
    }

}
