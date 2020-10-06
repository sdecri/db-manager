/**
 * DbManager.java
 */
package com.sdc.db_manager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.Map.Entry;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * @author Simone De Cristofaro
 * May 31, 2019
 */
public class DbManager {
    
    private static final Charset UTF8 = Charset.forName("UTF8");
    public static final String NEW_LINE = System.getProperty("line.separator");
    private static final WKTWriter WKT_WRITER = new WKTWriter();
    public static final int SRID = 4326;
    
    private static final String DEFAULT_POSTGRES_DB = "postgres";

    private static final String DB_EXISTS_QUERY = "SELECT count(datname)>0 FROM pg_catalog.pg_database WHERE lower(datname) = lower('%s')";
    
    public static final String TYPE_POSTGRESQL = "postgresql";
    
    private static final Logger LOG = LoggerFactory.getLogger(DbManager.class);

    static {
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
            LOG.error("Error loading drivers", e);
        }
    }
    
    private DbManagerContext context;
    
    public DbManager(DbManagerContext context) throws SQLException {
        
//        Map<String, String> properties = new HashMap<>();
//        properties.put("javax.persistence.jdbc.url", context.getJdbcUrl());
//        emf = Persistence.createEntityManagerFactory("map-matching-visualizer", properties);
//        em = emf.createEntityManager();
        this.context = context;
        

    }

    public void dropDb(String dbName) throws SQLException {
        
        LOG.info(String.format("Drop database %s", context.getDbName()));
        String jdbcUrl = buildPostgresqlConnectionStringforDefaultDb();
        try(Connection connection = DriverManager.getConnection(jdbcUrl); Statement statement = connection.createStatement()){
            statement.execute(String.format("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '%s';", dbName));
            statement.executeUpdate(String.format("Drop database %s", dbName));
        }

    }
    
    public void createDb(String dbName) throws SQLException {
        
        LOG.info(String.format("Create database \"%s\"", context.getDbName()));
        String jdbcUrl = buildPostgresqlConnectionStringforDefaultDb();
        try(Connection connection = DriverManager.getConnection(jdbcUrl); Statement statement = connection.createStatement()){
            statement.executeUpdate(String.format("Create database \"%s\"", dbName));
        }
        jdbcUrl = buildPostgresqlConnectionStringFromContext();
        try(Connection connection = DriverManager.getConnection(jdbcUrl); Statement statement = connection.createStatement()){
            statement.executeUpdate("create extension postgis");
        }

    }

    private String buildPostgresqlConnectionStringforDefaultDb() {

        return buildPostgresqlConnectionString(new DbManagerContext(context.getDbHost(), context.getDbPort(), DEFAULT_POSTGRES_DB, context.getDbUser(), context.getDbPassword()));
    }

    public boolean checkDbExist(String dbName) throws SQLException {
        
        String jdbcUrl = buildPostgresqlConnectionStringforDefaultDb();
        try(Connection connection = DriverManager.getConnection(jdbcUrl); Statement statement = connection.createStatement()){
            ResultSet rs = statement.executeQuery(String.format(DB_EXISTS_QUERY, dbName));
            rs.next();
            boolean result = rs.getBoolean(1);
            rs.close();
            return result;
        }
        
    }
    
    /**
     * Run an sql query
     * 
     * @param query
     */
    public void executeQuery(String query) {

        if(LOG.isDebugEnabled())
            LOG.debug("Execute query: " + query);
        try (Connection con = createConnectionFromContext();
                Statement ps = con.createStatement()) {

            ps.execute(query);
            if(LOG.isDebugEnabled())
                LOG.debug("Affected " + ps.getUpdateCount() + " rows");

        }
        catch (Exception e) {
            LOG.warn("Query Failed: " + query, e);
        }

    }
    
    public int executeUpdate(String query) {

        if(LOG.isDebugEnabled())
            LOG.debug("Execute query: " + query);
        try (Connection con = createConnectionFromContext();
                Statement ps = con.createStatement()) {
            int toReturn = ps.executeUpdate(query);
            if(LOG.isDebugEnabled()) {
                LOG.debug("Affected " + ps.getUpdateCount() + " rows, resultOfExecuteUpdate=" + toReturn);
            }
            return toReturn;
        } catch (Exception e) {
            LOG.warn("Query Failed: " + query, e);
        }
        return 0;
    }    
    
    /**
     * Execute the specified query
     * @param query
     * @return The {@link ResultSet} to use and the {@link Connection} that must be closed after using the {@link ResultSet}
     */
    public ExecuteQueryResult executeQueryAndGetResult(String query) {

        ExecuteQueryResult toReturn = null;
        if(LOG.isDebugEnabled())
            LOG.debug("Execute query: " + query);
        Connection con = null;
        Statement st = null;
        
        try {
            con = createConnectionFromContext();
            st = con.createStatement();
            ResultSet rs = st.executeQuery(query);
            toReturn = new ExecuteQueryResult(con, st, rs);
        }
        catch (Exception e) {
            LOG.warn("Query Failed: " + query, e);
            try {
                if(st!=null)
                    st.close();                
                if(con!=null)
                    con.close();
            }
            catch (SQLException e1) {
                LOG.error("Error closing connection");
            }
        }        
        return toReturn;
    }    
    
    public <T> List<T> executeQueryAndGetResult(String selectQuery, CheckedFunction<ResultSet, T> function) {

        List<T> actual = new ArrayList<>();

        try (Connection connection = createConnectionFromContext()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(selectQuery);
                while (rs.next()) {
                    T element = function.apply(rs);
                    actual.add(element);
                }
            }
        }
        catch (SQLException e) {
            String message = String.format("Failed running query", selectQuery);
            LOG.warn(message, e);
            throw new RuntimeException(message + " - " + e.getMessage(), e);

        }
        return actual;
    }

    private Connection createConnectionFromContext() throws SQLException {

        return DriverManager.getConnection(buildPostgresqlConnectionStringFromContext());
    }

    public void runSqlFile(File file) throws IOException {
        runSqlFile(file, null);
    }
    
    public void runSqlFile(File file, Map<String, String> parameters) throws IOException {
        LOG.info(String.format("Run sql file: %s", file.getAbsolutePath()));
        List<String> lines = Files.readAllLines(file.toPath(), UTF8);
        runSqlFileCore(lines, parameters);
        
    }

    public void runInputStream(InputStream is, Map<String, String> parameters) throws IOException {
        List<String> lines = readFile(is);
        runSqlFileCore(lines, parameters);
        
    }
    
    public static List<String> readFile(InputStream is) throws IOException{
        
        List<String> elements = new ArrayList<>();

        try (Scanner sc = new Scanner(is, "UTF-8")) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                elements.add(line);
            }

            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        }

        return elements;
    }
    
    public void runInputStream(InputStream is) throws IOException {
        runInputStream(is, null);
        
    }
    
    public void runSqlFileCore(List<String> lines, Map<String, String> parameters) throws IOException {
        String query = String.join(NEW_LINE, lines);
        if(parameters != null) {
            for (Entry<String, String> entry: parameters.entrySet()) {
                query = query.replaceAll(entry.getKey(), entry.getValue());
            }   
        }
//        executeQuery("drop table if exists mdf_link;create table mdf_links (id integer);");
        executeQuery(query);
        
    }
    

    public void copyFromCsv(String table, File inputDataFile) {

        String sql = String.format("COPY %s FROM stdin CSV HEADER DELIMITER ','", table);

        if(LOG.isDebugEnabled())
            LOG.debug(String.format("Run copy from csv query: %s; input file: %s", sql, inputDataFile));
        
        try (Connection connection = createConnectionFromContext();
                org.postgresql.core.BaseConnection pgcon = (org.postgresql.core.BaseConnection) connection;) {
            org.postgresql.copy.CopyManager mgr = new org.postgresql.copy.CopyManager(pgcon);
            java.io.Reader in = new java.io.BufferedReader(new java.io.FileReader(inputDataFile));
            long rowsaffected = mgr.copyIn(sql, in);
            if(LOG.isDebugEnabled())
                LOG.debug(String.format("Affected rows copying from csv: %d", rowsaffected));
        }
        catch (SQLException | IOException ex) {
            LOG.error("Error copying form csv", ex);
        }
    }
    
    
    /**
     * Check if the specified table exists in the specified schema
     * 
     * @param connection
     * @param tableName
     * @param tableSchema
     * @return <code>true</code> if the table exists
     * @throws SQLException
     */
    public boolean checkTableExist(Connection connection, String tableName, String tableSchema) throws SQLException {
    
        Statement stmt = connection.createStatement();
        String sql = createTableExistQuery(tableName, tableSchema);
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next())
            return rs != null && rs.getBoolean(1);
        return false;
    }
    

    public long count(String table) {
        return count(table, null);
    }
    
    public long count(String table, String where) {
        String whereClause = "";
        if(where != null)
            whereClause = String.format(" WHERE %s", where);
        String query = String.format("SELECT count(*) from %s %s", table, whereClause);

        return executeQueryAndGetResult(query, rs -> rs.getLong(1)).get(0);
    }

    public <I extends Insertable> void insert(String table, List<I> values) {
        
        insert(table
                , values.stream().map(Insertable::toObjectArray).collect(Collectors.toList())
                , null, SRID);
    }
    
    public void insert(String table, List<Object[]> values, List<String> columns, Integer srid) {
        
        String baseSql = null;
        if(columns != null && !columns.isEmpty()) {
            String columnsString = String.join(",", columns);
            baseSql = String.format("insert into %s (%s) values(%%s)", table, columnsString);
        }
        else
            baseSql = String.format("insert into %s values(%%s)", table);
        
        Connection con = null;
        Statement st = null;
        try {
            
            con = createConnectionFromContext();
            st = con.createStatement();
            con.setAutoCommit(false);

            for (Object[] row : values) {
                
                
                List<String> valuesToInsert = new ArrayList<>(row.length);
                for (Object value : row) {
                    
                    String valueToInsert = null;
                    if(value instanceof String) 
                        valueToInsert = "'" + value + "'";
                    else if (value instanceof Geometry)
                        valueToInsert = String.format("ST_GeomFromText('%s', %d)", WKT_WRITER.write((Geometry) value), srid);
                    else
                        valueToInsert = Objects.toString(value);
                    valuesToInsert.add(valueToInsert);
                }

                String valuesToInsertString = String.join(",", valuesToInsert);
                
                String sql = String.format(baseSql, valuesToInsertString);
                
                if(LOG.isDebugEnabled())
                    LOG.debug(String.format("Insert query: %s", sql));
                
                st.addBatch(sql);
                
            }
            
            st.executeBatch();
            con.commit();

        }
        catch (Exception e) {
            LOG.warn("Isert Failed: " + baseSql, e);
            if(con != null)
                try {
                    con.rollback();
                }
                catch (SQLException e1) {
                    LOG.error("Error rollbacking connection", e);
                }
        }finally {
            if(st != null)
                try {
                    st.close();
                }
                catch (SQLException e1) {
                    LOG.error("Error closing statement", e1);
                }
            if(con != null)
                try {
                    con.close();
                }
                catch (SQLException e) {
                    LOG.error("Error closing connection", e);
                }
        }
        
        
    }
    
    
    public static String createTableExistQuery(String tableName, String tableSchema) {

        String sql = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = '" + tableSchema + "' AND table_name = '" + tableName
                + "')";
        return sql;
    }

    private String buildPostgresqlConnectionStringFromContext() {
        return buildPostgresqlConnectionString(context);
    }
    
    public static String buildConnectionString(String type, DbManagerContext dbContext) {
        
        return String.format("jdbc:%s://%s:%d/%s?user=%s&password=%s", type, dbContext.getDbHost(), dbContext.getDbPort()
                , dbContext.getDbName(), dbContext.getDbUser(), dbContext.getDbPassword());
        
    }
    
    public static String buildPostgresqlConnectionString(DbManagerContext dbContext) {
        
        return buildConnectionString(TYPE_POSTGRESQL, dbContext);
        
    }

    public static Properties createConnectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.put("driver", "org.postgresql.Driver");
        return connectionProperties;
    }
    
    /**
     * Used to let that the apply method of the function throws an exception
     * @author simone.decristofaro
     * Nov 9, 2016
     * @param <T>
     * @param <R>
     */
    @FunctionalInterface
    public static interface CheckedFunction<T, R> {
        R apply(T t) throws SQLException;
    }

    
    public static class ExecuteQueryResult implements AutoCloseable{
        Connection connection;
        Statement statement;
        ResultSet resultSet;
        
        public ExecuteQueryResult(Connection connection, Statement statement, ResultSet resultSet) {
            super();
            this.connection = connection;
            this.statement = statement;
            this.resultSet = resultSet;
        }
        
        public Connection getConnection() {
        
            return connection;
        }
        
        public ResultSet getResultSet() {
        
            return resultSet;
        }

        public Statement getStatement() {
        
            return statement;
        }        
        
        /**
        * {@inheritDoc}
        */
        @Override
        public void close() throws SQLException {

            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
            
        }

        
    }
    
}
