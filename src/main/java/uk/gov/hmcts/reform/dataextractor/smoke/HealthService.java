package uk.gov.hmcts.reform.dataextractor.smoke;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmcts.reform.dataextractor.service.CaseDataService;
import uk.gov.hmcts.reform.dataextractor.service.impl.BlobServiceImpl;
import uk.gov.hmcts.reform.mi.micore.component.HealthCheck;
import uk.gov.hmcts.reform.mi.micore.exception.ServiceNotAvailableException;

@Component
@Slf4j
@RequiredArgsConstructor
public class HealthService implements HealthCheck {

    @Autowired
    private BlobServiceImpl blobService;

    @Autowired
    private CaseDataService caseDataService;

    @Override
    @SuppressWarnings("java:S899")
    public void check() throws ServiceNotAvailableException {
        try {
            blobService.listContainers().iterator().hasNext();
            caseDataService.checkConnection();
            log.info("Health check completed");
        } catch (Exception e) {
            throw new ServiceNotAvailableException("Not able to connect to dependency", e);
        }
    }
}
