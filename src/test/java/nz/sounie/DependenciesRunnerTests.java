package nz.sounie;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

// NB: Docker needs to be running in order for this test to be runnable.
@Testcontainers
class DependenciesRunnerTests {
    private static final String DB_USER = "db-user";
    private static final String PASSWORD = "aBcDeFg54321";

    // Annotation applied to hook into TestContainers lifecycle management.
    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("sampleDB")
            .withUsername(DB_USER)
            .withPassword(PASSWORD);

    @Test
    void testDependencies() throws Exception {
        assertThat(postgres.isRunning()).isTrue();
        assertThat(postgres.getHost()).isNotEmpty();
        assertThat(postgres.getFirstMappedPort()).isGreaterThan(0);

        String jdbcUrl = postgres.getJdbcUrl();
        System.out.println("JDBC URL: " + jdbcUrl);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            createTable(connection);

            UUID id = UUID.randomUUID();
            insertRow(connection, id, "First event", 1);

            readRow(connection, id);

            outputMetadata(connection);
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

    private void readRow(Connection connection, UUID expectedId) throws SQLException {
        try (PreparedStatement readStatement = connection.prepareStatement("SELECT * from event")) {
            // assert that one result and has name of "First event"
            readStatement.execute();

            try (ResultSet resultSet = readStatement.getResultSet()) {
                boolean firstResult = resultSet.next();
                assertThat(firstResult).isTrue();
                UUID id = resultSet.getObject("id", UUID.class);
                assertThat(id).isEqualTo(expectedId);
                assertThat(resultSet.getString("name")).isEqualTo("First event");

                // Verifying that no unexpected additional results are returned.
                boolean subsequentResult = resultSet.next();
                assertThat(subsequentResult).isFalse();
            }
        }
    }

    private void insertRow(Connection connection, UUID id, String name, int version) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO event (id, name, version) VALUES (?, ?, ?)")) {
            statement.setObject(1, id);
            statement.setString(2, name);
            statement.setInt(3, version);

            int rowCount = statement.executeUpdate();
            assertThat(rowCount).describedAs("Insert should make one row").isEqualTo(1);
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
