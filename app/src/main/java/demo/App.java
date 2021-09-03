package demo;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.FINEST);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(Level.FINEST);
        }


        try (var embeddedPostgres = EmbeddedPostgres.start()) {
            try (var conn = (PgConnection) DriverManager.getConnection(embeddedPostgres.getJdbcUrl("postgres", "postgres"))) {
                triggerBug(conn);
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void triggerBug(PgConnection conn) throws SQLException {
        conn.setAutoCommit(false);
        try (var statement = conn.createStatement()) {
            statement.execute("begin;");
            // Set transaction application_name to trigger ParameterStatus message after error
            // https://www.postgresql.org/docs/14/protocol-flow.html#PROTOCOL-ASYNC
            statement.execute("set application_name to 'app'");

            System.out.println(conn.getParameterStatuses());
            // Since to read an object that does not exist to trigger an error
            // There are probably other ways to do this but this is the one I stumbled upon
            // Fails with "org.postgresql.util.PSQLException: Unknown Response Type S."
            var loManager = new LargeObjectManager(conn);
            loManager.open(0L);
        } catch (SQLException e) {
            // application_name should be reset to "PostgreSQL JDBC Driver" (but isn't)
            System.out.println(conn.getParameterStatuses());
            throw e;
        }
    }
}