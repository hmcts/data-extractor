
FROM hmctspublic.azurecr.io/base/java:openjdk-11-distroless-1.3

ARG DEV_MODE=true
COPY lib/applicationinsights-agent-2.4.0-BETA-SNAPSHOT.jar lib/AI-Agent.xml /opt/app/
COPY build/libs/data-extractor-1.0.0.jar /opt/app/

CMD ["data-extractor-1.0.0.jar"]
