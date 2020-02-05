package uk.gov.hmcts.reform.dataextractor.utils;

public final class TestConstants {

    public static final String DB_CONNECTION_QUERY = "SELECT ID, NAME FROM case_data WHERE ID = 1";

    public static final String DB_DATA_QUERY = "SELECT P.ID, P.NAME, C.ID as \"child id\", C.NAME as \"child,name\" "
        + "FROM case_data P JOIN case_event C on P.ID = C.case_data_id WHERE P.ID = 1";

    public static final String AZURE_TEST_CONTAINER_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.4.0";

    public static final String DEFAULT_COMMAND = "azurite -l /data --blobHost 0.0.0.0 --loose";

    private TestConstants() {

    }
}
