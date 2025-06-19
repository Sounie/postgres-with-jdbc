package nz.sounie;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

// NB: Docker needs to be running in order for this test to be runnable.
@Testcontainers
class DatabaseUpsertTests {
    private static final String DB_USER = "db-user";
    private static final String PASSWORD = "aBcDeFg54321";

    // Annotation applied to hook into TestContainers lifecycle management.
    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("sampleDB")
            .withUsername(DB_USER)
            .withPassword(PASSWORD);

    @Test
    void testTransactionIsolationLevel() throws Exception {
        int transactionReadUncommited = Connection.TRANSACTION_READ_UNCOMMITTED;

        assertThat(postgres.isRunning()).isTrue();
        assertThat(postgres.getHost()).isNotEmpty();
        assertThat(postgres.getFirstMappedPort()).isGreaterThan(0);

        String jdbcUrl = postgres.getJdbcUrl();
        System.out.println("JDBC URL: " + jdbcUrl);

        // Set up table
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            createTable(connection);
        }

        UUID id = UUID.randomUUID();

        final int NUMBER_OF_UPSERTERS = 100;
        List<Upserter> upserters = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_UPSERTERS; i++) {
            Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD);
            connection.setAutoCommit(false);

            connection.setTransactionIsolation(transactionReadUncommited);
            Upserter upserter = new Upserter(connection, id, "First event", i + 1);
            upserters.add(upserter);
        }

        // Shuffle the ordering of elements in the upserters list
        Collections.shuffle(upserters);

        // Set up a concurrent executor to perform the upsert calls concurrently
        try (ExecutorService executorService =  Executors.newFixedThreadPool(NUMBER_OF_UPSERTERS)) {
            for (Upserter upserter : upserters) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        // Sleeping to allow ExecutorService to accumulate upserters before they run
                        try {
                            Thread.sleep(200L);
                        } catch (InterruptedException e) {
                            System.out.println("Sleep interrupted");
                        }
                        upserter.performUpsert();
                        if (!upserter.isSuccess()) {
                            System.err.println("Upsert failed");
                        }

                        upserter.closeConnection();
                    }
                });
            }
        }

        // Wait for all upserters to finish.)

        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            readRow(connection, id, NUMBER_OF_UPSERTERS);
        }
    }

    // This is how we could call a function, but we will avoid that while the function cannot distinguish versions
    private void upsertRowUsingFunction(Connection connection, UUID id, String name, int version)
    throws SQLException{
        try (PreparedStatement functionCallStatement = connection.prepareStatement("SELECT updateELseInsert(?, ?, ?)")) {
            functionCallStatement.setObject(1, id);
            functionCallStatement.setString(2, name);
            functionCallStatement.setInt(3, version);

            functionCallStatement.execute();
        }
    }

    private void setUpUpdateBeforeInsertFunction(Connection connection) throws SQLException {

/* Just trying out using a function to attempt update before insert.
   The fatal limitation here is when version supplied is lower than the existing version, as that will result in no result
   being found for the update, and an attempt to insert, that will fail - resulting in looping without an exit.
*/

        try (PreparedStatement createFunctionStatement = connection.prepareStatement(
                """
CREATE FUNCTION updateElseInsert(idParam UUID, nameParam varchar(255), versionParam bigint) RETURNS VOID AS
$$
  BEGIN
      LOOP
          UPDATE event SET name = nameParam, version = versionParam WHERE id = idParam AND event.version < versionParam;
          IF found THEN
            RETURN;
          END IF;
          -- TODO: Here should
          -- No record existed, so attempt insert
          BEGIN
            INSERT INTO event(id, name, version) values (idParam, nameParam, versionParam);
            RETURN;
          EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
          END;
      END LOOP;
  END;
  $$
  LANGUAGE plpgsql;
"""
        )) {
            createFunctionStatement.executeUpdate();
        }
    }

    private void outputMetadata(Connection connection) throws SQLException {
        /* In a real environment metadata may be useful for ensuring that the provisioned resource
        that is connected to has the expected capabilities.
        */
        DatabaseMetaData metadata = connection.getMetaData();

        int maxConnections = metadata.getMaxConnections();
        System.out.println("Max connections: " + maxConnections);

        String systemFunctions = metadata.getSystemFunctions();
        System.out.println("System functions: " + systemFunctions);

        String timeDateFunctions = metadata.getTimeDateFunctions();
        System.out.println("Time date functions: " + timeDateFunctions);
    }

    private void readRow(Connection connection, UUID expectedId, int expectedVersion) throws SQLException {
        try (PreparedStatement readStatement = connection.prepareStatement("SELECT id, name, version from event")) {
            // assert that one result and has name of "First event"
            readStatement.execute();

            try (ResultSet resultSet = readStatement.getResultSet()) {
                boolean firstResult = resultSet.next();
                assertThat(firstResult).isTrue();
                UUID id = resultSet.getObject("id", UUID.class);
                int version = resultSet.getInt("version");
                assertThat(id).isEqualTo(expectedId);
                assertThat(version).isEqualTo(expectedVersion);
                assertThat(resultSet.getString("name")).isEqualTo("First event");

                // Verifying that no unexpected additional results are returned.
                boolean subsequentResult = resultSet.next();
                assertThat(subsequentResult).isFalse();
            }
        }
    }

    private void upsertRow(Connection connection, UUID id, String name, int version) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO event (id, name, version) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = ?, version = ? " +
                "WHERE event.version < ?")) {
            statement.setObject(1, id);
            statement.setString(2, name);
            statement.setInt(3, version);

            statement.setString(4, name);
            statement.setInt(5, version);

            // Only update if the existing version is less than the current version.
            statement.setInt(6, version);

            statement.executeUpdate();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE event (id UUID PRIMARY KEY, " +
                    "name varchar(255) NOT NULL," +
                    "version bigint NOT NULL DEFAULT 0)");
        }
    }
}
