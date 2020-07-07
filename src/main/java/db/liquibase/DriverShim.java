package db.liquibase;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

class DriverShim implements Driver {
    private final Driver _driver;

    private DriverShim(Driver d) {
        this._driver = d;
    }

    public static DriverShim getInstance(Driver d){
        return new DriverShim(d);
    }

    public boolean acceptsURL(String u) throws SQLException {
        return this._driver.acceptsURL(u);
    }
    public Connection connect(String u, Properties p) throws SQLException {
        return this._driver.connect(u, p);
    }

    public int getMajorVersion() {
        return this._driver.getMajorVersion();
    }

    public int getMinorVersion() {
        return this._driver.getMinorVersion();
    }

    public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
        return this._driver.getPropertyInfo(u, p);
    }

    public boolean jdbcCompliant() {
        return this._driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
