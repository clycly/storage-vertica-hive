package util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcPool implements DataSource {

    private static final Log log = LogFactory.getLog(JdbcPool.class);

    private static Deque<Connection> deque = new LinkedList<>();

    private static Deque<Connection> busyQueue = new LinkedList<>();

    private static int poolInitSize;

    static {
        InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
        Properties prop = new Properties();
        try {
            prop.load(in);
            String url = prop.getProperty("url");
            int initSize = Integer.parseInt(prop.getProperty("initSize"));
            poolInitSize = initSize;
            String driver = prop.getProperty("driver");
            Class.forName(driver);
            for (int i = 0; i < initSize; i++) {
                Connection conn = DriverManager.getConnection(url, prop);
                log.info("获取到了连接:" + conn);
                deque.add(conn);
            }
        } catch (Exception e) {
            log.error("初始化vertica连接异常", e);
            throw new ExceptionInInitializerError(e);
        }
    }


    @Override
    public synchronized Connection getConnection() {
        if (deque.size() > 0) {
            Connection conn = deque.removeFirst();
            try {
                while (!deque.isEmpty() && !conn.isValid(1)) {
                    conn = deque.removeFirst();
                }
                //说明全部过期了，重新初始化
                if (conn.isValid(1)) {
                    InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
                    Properties prop = new Properties();
                    try {
                        prop.load(in);
                        String url = prop.getProperty("url");
                        int initSize = Integer.parseInt(prop.getProperty("initSize"));
                        String driver = prop.getProperty("driver");
                        Class.forName(driver);
                        for (int i = 0; i < initSize; i++) {
                            Connection newConn = DriverManager.getConnection(url, prop);
                            log.info("全部过期重新获取到了连接:" + newConn);
                            deque.add(newConn);
                        }
                        conn = deque.removeFirst();
                        busyQueue.add(conn);
                        return conn;
                    } catch (Exception e) {
                        log.error("初始化vertica连接异常", e);
                        throw new ExceptionInInitializerError(e);
                    }
                } else {
                    busyQueue.add(conn);
                    return conn;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (busyQueue.size() < poolInitSize) {
            InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            try {
                prop.load(in);
                String url = prop.getProperty("url");
                String driver = prop.getProperty("driver");
                Class.forName(driver);
                for (int i = 0; i < poolInitSize - busyQueue.size(); i++) {
                    Connection newConn = DriverManager.getConnection(url, prop);
                    log.info("补获取连接:" + newConn + "并加入连接池");
                    deque.add(newConn);
                }
                Connection againConn = deque.removeFirst();
                busyQueue.add(againConn);
                return againConn;
            } catch (Exception e) {
                log.error("补初始化vertica连接异常", e);
                throw new ExceptionInInitializerError(e);
            }
        }
        throw new RuntimeException("数据库正忙，无剩余连接可用");
    }

    public synchronized void returnToQueue(Connection conn) throws Exception {
        if (conn.isValid(1)) {
            log.info(conn + "还给连接池");
            deque.add(conn);
            log.info("连接池大小：" + deque.size());
        } else {
            InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            try {
                prop.load(in);
                String url = prop.getProperty("url");
                String driver = prop.getProperty("driver");
                Class.forName(driver);
                Connection newConn = DriverManager.getConnection(url, prop);
                log.info("重新获取连接:" + newConn + "并加入连接池");
                deque.add(newConn);
            } catch (Exception e) {
                log.error("重新初始化vertica连接异常", e);
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
