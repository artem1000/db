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
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.System.exit;

/* Auto-generates Change Log. Works for MS-SQL and Oracle */

public class LiquiBaseConnector {
    private static final String SRC_URL = "s_connection";
    private static final String SRC_USERNAME = "s_user";
    private static final String SRC_PSWD = "s_password";
    private static final String SRC_DB_NAME = "s_database";
    private static final String SRC_DB_SCHEMA = "s_schema";
    private static final String TRGT_DRIVER = "t_driver";
    private static final String TRGT_DRIVER_PATH = "t_driver_path";

    private static final String CHANGELOG_LOC = "src/main/resources/LiquiBaseChangeLog.xml";
    private static final Logger _logger = LogManager.getLogger(LiquiBaseConnector.class);
    private static final MetricRegistry _metrics = new MetricRegistry();
    private static final Timer _liquiBaseTimer = _metrics.timer(name(LiquiBaseConnector.class, SRC_URL));
    private static LiquiBaseConnector.OptionsHelper optionsHelper;

    public static void main(String[] args) {

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
        optionsHelper = new LiquiBaseConnector.OptionsHelper(args);
        _logger.info("Initiating connection to {} DB with userName {}, password {}", optionsHelper.getOptionValue(SRC_URL), optionsHelper.getOptionValue(SRC_USERNAME), optionsHelper.getOptionValue(SRC_PSWD));
       initializeDriver();

        liftSchemaFromSource();
        _logger.info("MeanRate for time to complete generating changeLog {} s, over {} events", _liquiBaseTimer.getMeanRate(), _liquiBaseTimer.getCount());
    }

    private static void initializeDriver() {
        try {
            URL u = new URL(optionsHelper.getOptionValue(TRGT_DRIVER_PATH));
            URLClassLoader ucl = new URLClassLoader(new URL[] {u});
            Driver d = (Driver)Class.forName(optionsHelper.getOptionValue(TRGT_DRIVER), true, ucl).newInstance();
            DriverManager.registerDriver(new DriverShim(d));
            _logger.info("Driver {} was initialized OK", optionsHelper.getOptionValue(TRGT_DRIVER));
        } catch (Exception e) {
            _logger.error("Exception caught", e);
        }
    }

    private static void liftSchemaFromSource() {
        String connectString = optionsHelper.getOptionValue(SRC_URL);
        if (optionsHelper.getOptionValue(SRC_DB_NAME) != null){
            connectString = optionsHelper.getOptionValue(SRC_URL) + "; database="+ optionsHelper.getOptionValue(SRC_DB_NAME)+ ";"; // We need to be within the database of interest in order to lift the schema
        }
        try (java.sql.Connection connection = DriverManager.getConnection(connectString, optionsHelper.getOptionValue(SRC_USERNAME), optionsHelper.getOptionValue(SRC_PSWD));
             final Timer.Context ignored = _liquiBaseTimer.time()){
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            generateChangeLog(database, optionsHelper.getOptionValue(SRC_DB_NAME), optionsHelper.getOptionValue(SRC_DB_SCHEMA));
        } catch (Exception e) {
            _logger.error("Exception caught when lifting the schema", e);
        }
    }

    private static void generateChangeLog(Database database, String databaseName, String schemaName) {
        try (Liquibase liquibase = new Liquibase(CHANGELOG_LOC, new FileSystemResourceAccessor(), database)){
            CatalogAndSchema catalogAndSchema = new CatalogAndSchema(databaseName, schemaName);
            DiffOutputControl diffOutputControl = new DiffOutputControl();
            DiffToChangeLog writer = new DiffToChangeLog(diffOutputControl);
            PrintStream pw = new PrintStream(new File(CHANGELOG_LOC));
            liquibase.generateChangeLog(catalogAndSchema, writer, pw);
        } catch (Exception e) {
            _logger.error("Exception caught when generating ChangeLog", e);
        }
    }

    static class OptionsHelper {
        private final Options _opts;
        private final CommandLineParser _parser;
        private CommandLine _cmd;

        OptionsHelper(String[] args) {
            _opts = new Options();
            _parser = new DefaultParser();

            Option urlOption = new Option("sc", SRC_URL, true, "Connection URL for the DB");
            urlOption.setRequired(true);
            _opts.addOption(urlOption);

            Option userOption = new Option("su", SRC_USERNAME, true, "User name");
            userOption.setRequired(true);
            _opts.addOption(userOption);

            Option dbnameOption = new Option("sd", SRC_DB_NAME, true, "Database");
            dbnameOption.setRequired(false);
            _opts.addOption(dbnameOption);

            Option schemaNameOption = new Option("ss", SRC_DB_SCHEMA, true, "Schema");
            schemaNameOption.setRequired(true);
            _opts.addOption(schemaNameOption);

            Option passwordOption = new Option("sp", SRC_PSWD, true, "Password");
            passwordOption.setRequired(true);
            _opts.addOption(passwordOption);

            Option tDriverOption = new Option("tdrv", TRGT_DRIVER, true, "Target Driver");
            tDriverOption.setRequired(true);
            _opts.addOption(tDriverOption);

            Option tDriverPathOption = new Option("tdrvp", TRGT_DRIVER_PATH, true, "Target Driver Path");
            tDriverPathOption.setRequired(true);
            _opts.addOption(tDriverPathOption);


            parseOptions(args);
        }

        private void parseOptions(String[] args) {
            try {
                _cmd = _parser.parse(_opts, args);
            } catch (ParseException e) {
                _logger.error("Error parsing the command line options", e);
                exit(-2);
            }
        }
        String getOptionValue (String optionLabel){
            return _cmd.getOptionValue(optionLabel);
        }
    }
}
