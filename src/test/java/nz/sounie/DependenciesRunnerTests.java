package nz.sounie;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

import static org.assertj.core.api.Assertions.assertThat;

// NB: Docker needs to be running in order for this test to be runnable.
@Testcontainers
class DependenciesRunnerTests {
    private static final String DB_USER = "db-user";
    private static final String PASSWORD = "aBcDeFg54321";

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

            insertRow(connection);

            readRow(connection);

            getMetadata(connection);
        }
    }

    private void getMetadata(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();

        int maxConnections = metadata.getMaxConnections();
        System.out.println("Max connections: " + maxConnections);

        String systemFunctions = metadata.getSystemFunctions();
        System.out.println("System functions: " + systemFunctions);

        String timeDateFunctions = metadata.getTimeDateFunctions();
        System.out.println("Time date functions: " + timeDateFunctions);
    }

    private void readRow(Connection connection) throws SQLException {
        try (PreparedStatement readStatement = connection.prepareStatement("SELECT * from event")) {
            // assert that one result and has name of "First event"
            readStatement.execute();

            try (ResultSet resultSet = readStatement.getResultSet()) {
                boolean firstResult = resultSet.next();
                assertThat(firstResult).isTrue();
                int id = resultSet.getInt("id");
                assertThat(id).isGreaterThan(0);
                assertThat(resultSet.getString("name")).isEqualTo("First event");

                // Verifying that no unexpected additional results are returned.
                boolean subsequentResult = resultSet.next();
                assertThat(subsequentResult).isFalse();
            }
        }
    }

    private void insertRow(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO event (name) VALUES (?)")) {
            statement.setString(1, "First event");

            int rowCount = statement.executeUpdate();
            assertThat(rowCount).describedAs("Insert should make one row").isEqualTo(1);
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE event (id serial PRIMARY KEY, " +
                    "name varchar(255) NOT NULL)");
        }
    }
}
