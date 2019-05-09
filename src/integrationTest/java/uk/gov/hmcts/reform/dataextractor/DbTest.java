package uk.gov.hmcts.reform.dataextractor;

import com.ninja_squad.dbsetup.DbSetup;
import com.ninja_squad.dbsetup.destination.DriverManagerDestination;
import com.ninja_squad.dbsetup.operation.Operation;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.gov.hmcts.reform.dataextractor.utils.PostgresqlBinderConfiguration;

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
                            "CREATE TABLE parent ("
                                    + "  id INTEGER PRIMARY KEY,"
                                    + "  name VARCHAR(50) NOT NULL,"
                                    + "  created_date TIMESTAMP NOT NULL"
                                    + ");",
                            "CREATE TABLE child ("
                                    + "  id INTEGER PRIMARY KEY,"
                                    + "  name VARCHAR(50) NOT NULL,"
                                    + "  data jsonb NOT NULL,"
                                    + "  created_date TIMESTAMP NOT NULL,"
                                    + "  parent_id INTEGER REFERENCES parent(id)"
                                    + ");"
                    ));

    public static final Operation INSERT_REFERENCE_DATA =
            sequenceOf(
                    insertInto("parent")
                            .columns("id", "name", "created_date")
                            .values(1, "A", "2019-04-12 23:45:45")
                            .values(2, "B", "2019-04-14 23:45:45")
                            .build(),
                    insertInto("child")
                            .columns("id", "name", "data", "created_date", "parent_id")
                            .values(1, "A1", "{\"a1Data\": {\"name\": \"A1\", \"subject\": "
                                    + "{\"name\": {\"lastName\": \"A1S\", \"firstName\": \"Harry\"}, "
                                    + "\"address\": \"a1Address\" }}}::json", "2019-04-12 23:45:46", 1)
                            .values(2, "A2", "{\"a2Data\": {\"name\": \"A2\", \"subject\": "
                                    + "{\"name\": {\"lastName\": \"A2S\", \"firstName\": \"Harry\"}, "
                                    + "\"address\": \"a2Address\" }}}::json", "2019-04-12 23:45:47",1)
                            .values(3, "B1", "{\"b1Data\": {\"name\": \"B2\", \"subject\": "
                                    + "{\"name\": {\"lastName\": \"B1S\", \"firstName\": \"Harry\"}, "
                                    + "\"address\": \"b1Address\" }}}::json", "2019-04-12 23:45:46", 2)
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
