package db.liquibase;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import liquibase.CatalogAndSchema;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.DiffToChangeLog;
import liquibase.exception.DatabaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.System.exit;

public class LiquiBaseCloneDb {

    private static final String DB_CHANGELOG_NAME = "MYDBCHANGELOG";
    private static final String DB_CHANGELOCK_NAME = "MYDBCHANGELOCK";
    private static final String SRC_URL = "s_connection";
    private static final String SRC_USERNAME = "s_user";
    private static final String SRC_PSWD = "s_password";
    private static final String SRC_DB_NAME = "s_database";
    private static final String SRC_DB_SCHEMA = "s_schema";
    private static final String TMP_PATH = "tmp_path";

    private static final Logger _logger = LogManager.getLogger(LiquiBaseCloneDb.class);
    private static final MetricRegistry _metrics = new MetricRegistry();
    private static final Timer _liquiBaseTimer = _metrics.timer(name(LiquiBaseCloneDb.class, SRC_URL));
    private static final boolean CLEAR_CHANGE_HISTORY = true;
    private static final boolean DELETE_CREATE_TARGET = true;
    private static File outputFile;
    private static OptionsHelper optionsHelper;

    /* Lifts the schema off source DB and clones it on the target DB. For this case source and target are on the same VM*/

    public static void main(String[] args) {
         org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
        optionsHelper = new OptionsHelper(args);

        _logger.info("Creating temp output file");
        createTmpOutputFile();
        if (outputFile != null) _logger.info("Created tmp output file at [{}]", outputFile.getAbsoluteFile());

        _logger.info("Initiating connection to {} DB with userName {}, password {}", optionsHelper.getOptionValue(SRC_URL), optionsHelper.getOptionValue(SRC_USERNAME), optionsHelper.getOptionValue(SRC_PSWD));
        liftSchemaFromSource();
        _logger.info("MeanRate for time to complete generating changeLog {} s, over {} events", _liquiBaseTimer.getMeanRate(), _liquiBaseTimer.getCount());

        _logger.info("Cloning...");
        spawnDb();
    }

    private static void createTmpOutputFile(){
        try {
            outputFile = File.createTempFile(optionsHelper.getOptionValue(SRC_DB_NAME), ".xml", new File(optionsHelper.getOptionValue(TMP_PATH)));
            outputFile.deleteOnExit();
        } catch (IOException e) {
            _logger.error("Exception caught during creating output file", e);
        }
    }

    private static void liftSchemaFromSource() {
        String connectString = optionsHelper.getOptionValue(SRC_URL) + "; database="+ optionsHelper.getOptionValue(SRC_DB_NAME)+ ";"; // We need to be within the database of interest in order to lift the schema
        try (java.sql.Connection connection = DriverManager.getConnection(connectString, optionsHelper.getOptionValue(SRC_USERNAME), optionsHelper.getOptionValue(SRC_PSWD)); final Timer.Context ignored = _liquiBaseTimer.time()){

            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            generateChangeLog(database, optionsHelper.getOptionValue(SRC_DB_NAME), optionsHelper.getOptionValue(SRC_DB_SCHEMA));

        } catch (Exception e) {
            _logger.error("Exception caught when lifting the schema", e);
        }
    }

    private static void generateChangeLog(Database database, String databaseName, String schemaName) {

        try (Liquibase liquibase = new Liquibase(outputFile.getAbsolutePath(), new FileSystemResourceAccessor(), database)){
            CatalogAndSchema catalogAndSchema = new CatalogAndSchema(databaseName, schemaName);
            DiffOutputControl diffOutputControl = new DiffOutputControl();
            DiffToChangeLog writer = new DiffToChangeLog(diffOutputControl);
            PrintStream pw = new PrintStream(outputFile);
            liquibase.generateChangeLog(catalogAndSchema, writer, pw);
        } catch (Exception e) {
            _logger.error("Exception caught", e);
        }
    }

    private static void spawnDb() {
        try (java.sql.Connection connection = DriverManager.getConnection(optionsHelper.getOptionValue(SRC_URL), optionsHelper.getOptionValue(SRC_USERNAME) , optionsHelper.getOptionValue(SRC_PSWD) ); // need to be in the high-level "default" db
             final Timer.Context ignored = _liquiBaseTimer.time()){
            if (!deleteLiquiBaseDbChangeLog(connection) || !deleteDbIfExists(connection, optionsHelper.getOptionValue(SRC_DB_NAME)) || !createDbIfNotExist(connection, optionsHelper.getOptionValue(SRC_DB_NAME))) {
                _logger.warn("Database may be in use, cannot access it. Aborting...");
                return;
            }
            createDbInstance(connection);
        } catch (Exception e) {
            _logger.error("Exception caught", e);
        }
    }

    private static boolean deleteDbIfExists(Connection connection, String databaseName) {
        if (!DELETE_CREATE_TARGET) return true;
        try (Statement stmt = connection.createStatement()) {
            _logger.info("Deleting database {} if it exists", databaseName);
            stmt.executeUpdate("if db_id('"+databaseName+"') is not null DROP DATABASE "+databaseName+";");
            return true;
        }catch (Exception e) {
            _logger.error("Exception caught during deleting a database {}", databaseName, e);
            return false;
        }
    }

    private static boolean deleteLiquiBaseDbChangeLog(Connection connection) {
        if (!CLEAR_CHANGE_HISTORY) return true;
        try (Statement stmt = connection.createStatement()) {
            String dropLogStatement = "if object_id('dbo."+DB_CHANGELOG_NAME+"') is not null DROP TABLE dbo."+DB_CHANGELOG_NAME+";";
            String dropLockStatement = "if object_id('dbo."+DB_CHANGELOCK_NAME+"') is not null DROP TABLE dbo."+DB_CHANGELOCK_NAME+";";
            _logger.info("Deleting LiquiBase change log with [{}] statement", dropLogStatement);
            _logger.info("Deleting LiquiBase change lock with [{}] statement", dropLockStatement);
            stmt.executeUpdate(dropLogStatement);
            stmt.executeUpdate(dropLockStatement);
            return true;
        }catch (Exception e) {
            _logger.error("Exception caught during creating a database", e);
            return false;
        }
    }

    private static boolean createDbIfNotExist(Connection connection, String databaseName) {
        if (!DELETE_CREATE_TARGET) return true;
        try (Statement stmt = connection.createStatement()) {
            _logger.info("Creaing a database {} if it does not exist", databaseName);
            stmt.executeUpdate("if db_id('"+databaseName+"') is null create DATABASE "+databaseName+";");
            return true;
        }catch (Exception e) {
            _logger.error("Exception caught during creating a database {}", databaseName, e);
            return false;
        }
    }

    private static void createDbInstance(Connection connection) {
        Database database = createLiquibaseDb(connection);
        if (database == null) {
            _logger.warn("Database was not created, exiting...");
            return;
        }
        try (Liquibase liquibase = new Liquibase(outputFile.getAbsolutePath(), new FileSystemResourceAccessor(), database)){
            _logger.info("Applying changes to the target database");
            liquibase.update(new Contexts(), new LabelExpression());
            deleteLiquiBaseDbChangeLog(connection);
        } catch (Exception e) {
            _logger.error("Exception caught during applying a change log", e);
        }
    }

    private static Database createLiquibaseDb(Connection connection) {
        Database database = null;
        try {
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection)); // hi-lvl connection to create new DB
            database.setDatabaseChangeLogTableName(DB_CHANGELOG_NAME);
            database.setDatabaseChangeLogLockTableName(DB_CHANGELOCK_NAME);
        } catch (DatabaseException e) {
            _logger.error("Exception caught during creating a Database", e);
        }
        return database;
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
            dbnameOption.setRequired(true);
            _opts.addOption(dbnameOption);

            Option schemaNameOption = new Option("ss", SRC_DB_SCHEMA, true, "Schema");
            schemaNameOption.setRequired(true);
            _opts.addOption(schemaNameOption);

            Option passwordOption = new Option("sp", SRC_PSWD, true, "Password");
            passwordOption.setRequired(true);
            _opts.addOption(passwordOption);

            Option tmpPathOption = new Option("tp", TMP_PATH, true, "Temporary File Path");
            tmpPathOption.setRequired(true);
            _opts.addOption(tmpPathOption);
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
