package db.flyway;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.System.exit;

/* Quick connection test of the FlyWay connecting to a MSSQL DB*/

public class FlyWayConnector {

    private static final String CONNECTION_STRING_LBL = "connection";
    private static final String USERNAME_LBL = "user";
    private static final String PASSWORD_LBL = "password";
    private static final Logger logger = LogManager.getLogger(FlyWayConnector.class);
    private static final MetricRegistry _metrics = new MetricRegistry();
    private static final Timer flyWayTimer = _metrics.timer(name(FlyWayConnector.class, CONNECTION_STRING_LBL));

    public static void main(String[] args) {

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        Options opts = new Options();

        Option urlOption = new Option("c", CONNECTION_STRING_LBL, true, "Connection URL for the DB");
        urlOption.setRequired(true);
        opts.addOption(urlOption);

        Option usrOption = new Option("u", USERNAME_LBL, true, "User name");
        usrOption.setRequired(true);
        opts.addOption(usrOption);

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

        if (connectString == null || userName == null || password == null){
            logger.error("One or more of the requirement arguments are missing. Make sure URL, User Name and Password are supplied as args");
            exit(-2);
        }

        logger.info("Initiating connection to {} DB with userName {}, password {}", connectString, userName, password);

        try (final Timer.Context ignore = flyWayTimer.time()){
            Flyway flyway = Flyway.configure().dataSource(connectString, userName, password).load();
            flyway.migrate();
        } catch (Exception e) {
            logger.error("Exception caught while executing FlyWay", e);
        }

        logger.info("MeanRate for time to complete connection and migration {} s, over {} events", flyWayTimer.getMeanRate(), flyWayTimer.getCount());
    }
}
