package uk.gov.hmcts.reform.dataextractor;

import com.microsoft.applicationinsights.TelemetryClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ApplicationContext;

import uk.gov.hmcts.reform.dataextractor.task.PreExecutor;
import uk.gov.hmcts.reform.mi.micore.component.HealthCheck;

import java.util.Map;

@Slf4j
@SpringBootApplication(scanBasePackages = "uk.gov.hmcts.reform", exclude = {DataSourceAutoConfiguration.class})
public class DataExtractorApplication implements ApplicationRunner {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ExtractionComponent extractionComponent;

    @Autowired
    private HealthCheck healthCheck;

    @Autowired
    private TelemetryClient client;

    @Value("${smoke.test.enabled:false}")
    private boolean smokeTest;

    @Value("${telemetry.wait.period:10000}")
    private int waitPeriod;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try {
            if (smokeTest) {
                healthCheck.check();
            } else {
                runPreExecutionTasks();
                extractionComponent.execute();
            }
        } catch (Exception e) {
            log.error("Error executing integration service", e);
            throw e;
        } finally {
            client.flush();
            waitTelemetryGracefulPeriod();
        }
        log.info("CCD Data extractor process completed");
    }

    private void runPreExecutionTasks() {
        Map<String, PreExecutor> beans = context.getBeansOfType(PreExecutor.class);
        for (PreExecutor preExecutor : beans.values()) {
            if (preExecutor.isEnabled()) {
                preExecutor.execute();
            }
        }
    }

    private void waitTelemetryGracefulPeriod() throws InterruptedException {
        Thread.sleep(waitPeriod);
    }

    public static void main(String[] args) {
        SpringApplication.run(DataExtractorApplication.class);
    }

}
