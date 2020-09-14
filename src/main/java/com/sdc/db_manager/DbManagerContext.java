/**
 * DbManagerContext.java
 */
package com.sdc.db_manager;


/**
 * @author Simone De Cristofaro
 * Jun 3, 2019
 */
public class DbManagerContext {

    private String dbHost;
    private int dbPort;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    /**
     * @param dbHost
     * @param dbPort
     * @param dbName
     * @param dbUser
     * @param dbPassword
     */
    public DbManagerContext(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword) {

        super();
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }
    
    /**
     * @return the {@link DbManagerContext#dbHost}
     */
    public String getDbHost() {
    
        return dbHost;
    }
    
    /**
     * @param dbHost the {@link DbManagerContext#dbHost} to set
     */
    public void setDbHost(String dbHost) {
    
        this.dbHost = dbHost;
    }
    
    /**
     * @return the {@link DbManagerContext#dbPort}
     */
    public int getDbPort() {
    
        return dbPort;
    }
    
    /**
     * @param dbPort the {@link DbManagerContext#dbPort} to set
     */
    public void setDbPort(int dbPort) {
    
        this.dbPort = dbPort;
    }
    
    /**
     * @return the {@link DbManagerContext#dbName}
     */
    public String getDbName() {
    
        return dbName;
    }
    
    /**
     * @param dbName the {@link DbManagerContext#dbName} to set
     */
    public void setDbName(String dbName) {
    
        this.dbName = dbName;
    }
    
    /**
     * @return the {@link DbManagerContext#dbUser}
     */
    public String getDbUser() {
    
        return dbUser;
    }
    
    /**
     * @param dbUser the {@link DbManagerContext#dbUser} to set
     */
    public void setDbUser(String dbUser) {
    
        this.dbUser = dbUser;
    }
    
    /**
     * @return the {@link DbManagerContext#dbPassword}
     */
    public String getDbPassword() {
    
        return dbPassword;
    }
    
    /**
     * @param dbPassword the {@link DbManagerContext#dbPassword} to set
     */
    public void setDbPassword(String dbPassword) {
    
        this.dbPassword = dbPassword;
    }
    
    
    
}
