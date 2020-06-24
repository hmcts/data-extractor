package uk.gov.hmcts.reform.dataextractor;

import com.ninja_squad.dbsetup.DbSetup;
import com.ninja_squad.dbsetup.destination.DriverManagerDestination;
import com.ninja_squad.dbsetup.operation.Operation;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import uk.gov.hmcts.reform.dataextractor.utils.PostgresqlBinderConfiguration;
import uk.gov.hmcts.reform.dataextractor.utils.TestUtils;

import static com.ninja_squad.dbsetup.Operations.insertInto;
import static com.ninja_squad.dbsetup.Operations.sequenceOf;
import static com.ninja_squad.dbsetup.Operations.sql;


@Testcontainers
public class DbTest {

    @Container
    public static final PostgreSQLContainer postgresContainer = new PostgreSQLContainer("postgres:9.6.12-alpine");

    protected static String jdbcUrl;
    protected static String username;
    protected static String password;

    public static final Operation CREATE_TABLES =
        sequenceOf(
            sql(
                "CREATE TABLE case_data ("
                    + "  id INTEGER PRIMARY KEY,"
                    + "  reference INTEGER,"
                    + "  version INTEGER,"
                    + "  name VARCHAR(50) NOT NULL,"
                    + "  security_classification VARCHAR(200) NOT NULL,"
                    + "  jurisdiction VARCHAR(50) NOT NULL,"
                    + "  case_type_id VARCHAR(50) NOT NULL,"
                    + "  state VARCHAR(50) NOT NULL,"
                    + "  last_modified TIMESTAMP,"
                    + "  data jsonb NOT NULL,"
                    + "  data_classification JSONB NOT NULL,"
                    + "  last_state_modified_date TIMESTAMP,"
                    + "  created_date TIMESTAMP NOT NULL"
                    + ");",
                "CREATE TABLE case_event("
                    + "    id INTEGER PRIMARY KEY,"
                    + "    name VARCHAR(50) NOT NULL,"
                    + "    data jsonb NOT NULL,"
                    + "    data_classification JSONB NOT NULL,"
                    + "    created_date TIMESTAMP NOT NULL,"
                    + "    case_data_id INTEGER REFERENCES case_data(id),"
                    + "    case_type_version VARCHAR(50),"
                    + "    state_id  VARCHAR(50),"
                    + "    case_type_id VARCHAR(50),"
                    + "    state_name VARCHAR(50),"
                    + "    event_id  VARCHAR(50),"
                    + "    event_name VARCHAR(50),"
                    + "    user_id VARCHAR(100),"
                    + "    user_first_name VARCHAR(100),"
                    + "    user_last_name VARCHAR(100),"
                    + "    summary VARCHAR(100),"
                    + "    description VARCHAR(100),"
                    + "    security_classification VARCHAR(50)"
                    + ");"
            ));

    public static final Operation INSERT_REFERENCE_DATA =
        sequenceOf(
            insertInto("case_data")
                .columns("id", "name", "jurisdiction", "case_type_id", "last_modified", "state", "reference",
                    "security_classification", "created_date", "data", "data_classification")
                .values(1, "A", "jur", "test", "2019-04-12 23:45:45", "created", "1", "public", "2019-04-12 23:45:45",
                    TestUtils.getDataFromFile("dataA1.json"), TestUtils.getDataFromFile("dataA1.json"))
                .values(2, "B", "jur", "test", "2019-04-12 23:45:45", "created", "2", "public", "2019-04-14 23:45:45",
                    TestUtils.getDataFromFile("dataA1.json"), TestUtils.getDataFromFile("dataA1.json"))
                .build(),
            insertInto("case_event")
                .columns("id", "name", "created_date", "case_data_id", "case_type_id", "case_type_version",
                    "state_id", "security_classification", "state_name", "event_id", "event_name", "summary",
                    "description", "user_id", "user_first_name", "user_last_name", "data_classification", "data")
                .values(1, "A1", "2019-12-10 23:45:46", 1, "test", "v1", "created", "PUBLIC", "stateName", "eventId", "eventName", "summary",
                    "description", "userId", "userFirstName", "userLastName",
                    TestUtils.getDataFromFile("dataA1.json"), TestUtils.getDataFromFile("dataA1.json"))
                .values(2, "A2", "2019-12-10 23:45:47", 1, "test", "v1", "created", "PUBLIC", "stateName", "eventId", "eventName", "summary",
                    "description", "userId", "userFirstName", "userLastName",
                    TestUtils.getDataFromFile("dataA1.json"), TestUtils.getDataFromFile("dataA2.json"))
                .values(3, "B1", "2020-01-12 23:45:46", 2, "test", "v1", "created", "PUBLIC", "stateName", "eventId", "eventName", "summary",
                    "description", "userId", "userFirstName", "userLastName",
                    TestUtils.getDataFromFile("dataA1.json"), TestUtils.getDataFromFile("dataB1.json"))
                .build());

    @BeforeAll
    public static void init() {
        jdbcUrl = postgresContainer.getJdbcUrl();
        username = postgresContainer.getUsername();
        password = postgresContainer.getPassword();

        Operation populateDbOperation =
            sequenceOf(
                CREATE_TABLES,
                INSERT_REFERENCE_DATA
            );

        DbSetup dbSetup = new DbSetup(new DriverManagerDestination(jdbcUrl, username, password),
            populateDbOperation, new PostgresqlBinderConfiguration());
        dbSetup.launch();
    }

    protected DbTest() {
        // This constructor is intentionally empty. Nothing special is needed here.
    }

}
