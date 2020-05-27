package db.liquibase;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.DiffToChangeLog;
import liquibase.resource.FileSystemResourceAccessor;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.PrintStream;
import java.sql.DriverManager;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.System.exit;

public class LiquiBaseConnector {

    private static final String CONNECTION_STRING_LBL = "connection";
    private static final String LB_CHANGELOG = "src/main/resources/LiquiBaseChangeLog.xml";
    private static final String USERNAME_LBL = "user";
    private static final String PASSWORD_LBL = "password";
    private static final String DB_LBL = "database";
    private static final String SCHEMA_LBL = "schema";
    private static final Logger logger = LogManager.getLogger(LiquiBaseConnector.class);
    private static final MetricRegistry _metrics = new MetricRegistry();
    private static final Timer liquiBaseTimer = _metrics.timer(name(LiquiBaseConnector.class, CONNECTION_STRING_LBL));

    public static void main(String[] args) {

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        Options opts = new Options();

        Option urlOption = new Option("c", CONNECTION_STRING_LBL, true, "Connection URL for the DB");
        urlOption.setRequired(true);
        opts.addOption(urlOption);

        Option userOption = new Option("u", USERNAME_LBL, true, "User name");
        userOption.setRequired(true);
        opts.addOption(userOption);

        Option dbnameOption = new Option("d", DB_LBL, true, "Database");
        dbnameOption.setRequired(true);
        opts.addOption(dbnameOption);

        Option schemaNameOption = new Option("s", SCHEMA_LBL, true, "Schema");
        schemaNameOption.setRequired(true);
        opts.addOption(schemaNameOption);

        Option passwordOption = new Option("p", PASSWORD_LBL, true, "Password");
        passwordOption.setRequired(true);
        opts.addOption(passwordOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException e) {
            logger.error("Error parsing the command line options", e);
            exit(-2);
        }

        String connectString = cmd.getOptionValue(CONNECTION_STRING_LBL);
        String userName = cmd.getOptionValue(USERNAME_LBL);
        String password = cmd.getOptionValue(PASSWORD_LBL);
        String databaseName = cmd.getOptionValue(DB_LBL);
        String schemaName = cmd.getOptionValue(SCHEMA_LBL);

        if (connectString == null || userName == null || password == null || schemaName == null || databaseName == null){
            logger.error("One or more of the requirement arguments are missing. Make sure URL, User Name and Password are supplied as args");
            exit(-2);
        }

        logger.info("Initiating connection to {} DB with userName {}, password {}", connectString, userName, password);

        try (java.sql.Connection connection = DriverManager.getConnection(connectString, userName, password); final Timer.Context ignored = liquiBaseTimer.time()){
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            generateChangeLog(database, databaseName, schemaName);
        } catch (Exception e) {
            logger.error("Exception caught", e);
        }
        logger.info("MeanRate for time to complete generating changeLog {} s, over {} events", liquiBaseTimer.getMeanRate(), liquiBaseTimer.getCount());
    }

    private static void generateChangeLog(Database database, String databaseName, String schemaName) {

        try (Liquibase liquibase = new Liquibase(LB_CHANGELOG, new FileSystemResourceAccessor(), database)){
            CatalogAndSchema catalogAndSchema = new CatalogAndSchema(databaseName, schemaName);
            DiffOutputControl diffOutputControl = new DiffOutputControl();
            DiffToChangeLog writer = new DiffToChangeLog(diffOutputControl);
            PrintStream pw = new PrintStream(new File(LB_CHANGELOG));
            liquibase.generateChangeLog(catalogAndSchema, writer, pw);
        } catch (Exception e) {
            logger.error("Exception caught", e);
        }
    }
}
