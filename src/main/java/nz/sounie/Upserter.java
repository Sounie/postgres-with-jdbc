package nz.sounie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Self-contained worker object for upserting a specified version to a specified id.
 * <p>
 *     Intended to be used for trying out testing race conditions and the influence of transactions
 *     with different isolation levels.
 * </p>
 * <p>
 *     Documentation about isolation levels can be found at:
 *     https://www.postgresql.org/docs/current/transaction-iso.html
 * </p>
 */
public class Upserter {
    final Connection connection;
    final PreparedStatement statement;
    final int version;

    boolean success = false;

    /**
     *
     * @param connection with isolation level set up, based on options available from Connection
     * @param id
     * @param version
     */
    Upserter(Connection connection, UUID id, String name, int version) {
        try {
            this.connection = connection;
            this.version = version;
            this.statement =  connection.prepareStatement(
                    "INSERT INTO event (id, name, version) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = ?, version = ? " +
                    "WHERE event.version < ?");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to prepare statement", e);
        }

        try {
            statement.setObject(1, id);

            statement.setString(2, name);
            statement.setInt(3, version);

            statement.setString(4, name);
            statement.setInt(5, version);

            // Only update if the existing version is less than the current version.
            statement.setInt(6, version);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set up state for prepared statement.", e);
        }
    }

    void performUpsert()
    {
        try {
            statement.executeUpdate();

            connection.commit();

            this.success = true;
        } catch (SQLException e) {
            try {
                System.err.println("Failed to commit prepared statement, " + e.getMessage());
                connection.rollback();
            }  catch (SQLException ex) {
                System.err.println("Failure during rollback, " + ex.getMessage());
            }
            // Don't need to do anything here, as success state will remain false.
        } finally {
            try {
                System.out.println("Version " + version + " inserted / updated");
                statement.close();
            } catch (SQLException e) {
                // We don't regard this as the upsert failing.
                System.err.println("Exception while closing statement: " + e.getMessage());
            }
        }
    }

    public void closeConnection() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            System.err.println("Exception when  closing connection: " + e.getMessage());
        }
    }

    public boolean isSuccess() {
        return success;
    }
}
