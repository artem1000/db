package db.liquibase;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.apache.commons.cli.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.System.exit;

public class LiquiBaseCreateDb {

    private static final String DB_CHANGELOG_NAME = "MYDBCHANGELOG";
    private static final String DB_CHANGELOCK_NAME = "MYDBCHANGELOCK";
    private static final String TRGT_URL = "t_connection";
    private static final String TRGT_USER = "t_user";
    private static final String TRGT_PSWD = "t_password";
    private static final String TRGT_DBNAME = "t_database";
    private static final String TRGT_DRIVER = "t_driver";
    private static final String TRGT_DRIVER_PATH = "t_driver_path";

    private static final String CHANGELOG_LOC = "src/main/resources/LiquiBaseChangeLog_dbo.json";
    private static final Logger _logger = LogManager.getLogger(LiquiBaseCreateDb.class);
    private static final MetricRegistry _metrics = new MetricRegistry();
    private static final Timer _liquiBaseTimer = _metrics.timer(name(LiquiBaseCreateDb.class, TRGT_URL));
    private static final boolean CLEAR_CHANGE_HISTORY = true;
    private static final boolean DELETE_CREATE_TARGET = true;
    private static final List<String> SCHEMAS = new ArrayList<>();
    private static LiquiBaseCreateDb.OptionsHelper optionsHelper;

    /* Creates a DB and restoring schema using supplied Change Log */

    public static void main(String[] args) {
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
        optionsHelper = new LiquiBaseCreateDb.OptionsHelper(args);
        BasicConfigurator.configure();
        SCHEMAS.add("dbo");

        initializeDriver();
        _logger.info("Initiating connection to {} DB with userName {}, password {}", optionsHelper.getOptionValue(TRGT_URL), optionsHelper.getOptionValue(TRGT_USER) , optionsHelper.getOptionValue(TRGT_PSWD));
        spawnDb();
        _logger.info("MeanRate for time to complete generating changeLog {} s, over {} events", _liquiBaseTimer.getMeanRate(), _liquiBaseTimer.getCount());
    }

    private static void initializeDriver() {

        try (URLClassLoader ucl = new URLClassLoader(new URL[] {new URL(optionsHelper.getOptionValue(TRGT_DRIVER_PATH))})) {
            Driver d = (Driver)Class.forName(optionsHelper.getOptionValue(TRGT_DRIVER), true, ucl).newInstance();
            DriverManager.registerDriver(DriverShim.getInstance(d));
            _logger.info("Driver {} was initialized OK", optionsHelper.getOptionValue(TRGT_DRIVER));
        } catch (Exception e) {
            _logger.error("Exception caught", e);
        }
    }

    private static void spawnDb() {
        try (java.sql.Connection connection = DriverManager.getConnection(optionsHelper.getOptionValue(TRGT_URL), optionsHelper.getOptionValue(TRGT_USER) , optionsHelper.getOptionValue(TRGT_PSWD)); // need to be in the high-level "default" db
             final Timer.Context ignored = _liquiBaseTimer.time()){
            if (!deleteLiquiBaseDbChangeLog(connection) || !deleteDbIfExists(connection, optionsHelper.getOptionValue(TRGT_DBNAME)) || !createDbIfNotExist(connection, optionsHelper.getOptionValue(TRGT_DBNAME))) {
                _logger.warn("Database may be in use, cannot access it. Aborting...");
                return;
            }
            createSchemasIfNotExist(connection, optionsHelper.getOptionValue(TRGT_DBNAME));
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
            _logger.info("Creating a database {} if it does not exist", databaseName);
            stmt.executeUpdate("if db_id('"+databaseName+"') is null create DATABASE "+databaseName+";");
            return true;
        }catch (Exception e) {
            _logger.error("Exception caught during creating a database {}", databaseName, e);
            return false;
        }
    }

    private static void createSchemasIfNotExist(Connection connection, String dbName) {
        for (String schema : SCHEMAS) {
            try (Statement stmt = connection.createStatement()) {
                _logger.info("Creating a schema {} ", schema);
                stmt.executeUpdate("USE "+dbName+"; IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '"+schema+"') BEGIN EXEC (" +
                        "'CREATE SCHEMA "+schema+";') " +
//                        "CREATE TYPE Name FROM VARCHAR(50) " +
//                        "CREATE TYPE Flag FROM BIT " +
//                        "CREATE TYPE NameStyle FROM BIT " +
//                        "CREATE TYPE OrderNumber FROM NVARCHAR(25) " +
//                        "CREATE TYPE Phone FROM NVARCHAR(25) " +
//                        "CREATE TYPE AccountNumber FROM INT " +
                        "END;");
            }catch (Exception e) {
                _logger.error("Exception caught during creating a database {}", dbName, e);
            }
        }
    }

    private static void createDbInstance(Connection connection) {
        Database database = createLiquibaseDb(connection);
        if (database == null) {
            _logger.warn("Database was not created, exiting...");
            return;
        }
        try (Liquibase liquibase = new Liquibase(CHANGELOG_LOC, new FileSystemResourceAccessor(), database)){
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
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
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

            Option urlOption = new Option("tc", TRGT_URL, true, "Connection URL for the DB");
            urlOption.setRequired(true);
            _opts.addOption(urlOption);

            Option userOption = new Option("tu", TRGT_USER, true, "User name");
            userOption.setRequired(true);
            _opts.addOption(userOption);

            Option passwordOption = new Option("tp", TRGT_PSWD, true, "Password");
            passwordOption.setRequired(true);
            _opts.addOption(passwordOption);

            Option dbOption = new Option("td", TRGT_DBNAME, true, "Database");
            dbOption.setRequired(true);
            _opts.addOption(dbOption);

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
