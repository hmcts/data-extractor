package uk.gov.hmcts.reform.dataextractor.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("etl.db")
@Data
public class DbConfig {
    @Value("${base.dir:/mnt/secrets/}")
    private String baseDir;

    private String url;
    private String user;
    private String password;

    private String cloneUrl;
    private String cloneUser;
    private String clonePassword;
}
