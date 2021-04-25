package util;

import bean.ColumnData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class VerticaDbUtil {

    private static final Log log = LogFactory.getLog(VerticaDbUtil.class);

    private static JdbcPool jdbcPool = new JdbcPool();

    public static boolean execDDL(String sql) throws Exception {
        log.info("vertica [执行SQL语句]:" + sql);
        PreparedStatement statement = null;
        Connection conn = getConnection();
        try {
            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sql);
            int i = statement.executeUpdate();
            conn.commit();
            return i >= 0;
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            throw new Exception(e.getMessage());
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
            }
            release(conn, statement, null);
        }
    }

    public static ColumnData executeLimitSql(String sqls) throws Exception {
        String sql = "select a.* from (" + sqls + ")a limit 10000";
        return executeSql(sql);
    }

    public static void batchInsert(String insertSql, List<List<String>> dataList) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(insertSql);
            for (int i = 0; i < dataList.size(); i++) {
                List<String> tempList = dataList.get(i);
                for (int j = 0; j < tempList.size(); j++) {
                    pstmt.setObject(j + 1, tempList.get(j));
                }
            }
            pstmt.executeBatch();
            conn.commit();
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            throw new Exception(e.getMessage());
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
            }
            release(conn, pstmt, null);
        }
    }

    public static ColumnData executeSql(String sqls) throws SQLException {
        log.info("---- Begin executing sql:" + sqls + "----");
        LocalDateTime start = LocalDateTime.now();
        Connection conn = getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet;
        try {
            statement = conn.prepareStatement(sqls);
            statement.setMaxRows(10000);
            resultSet = statement.executeQuery();
        } catch (Exception var14) {
            log.error("executeSql error:", var14);
            release(conn, statement, null);
            throw new SQLException(var14.getMessage());
        }

        log.info("vertica sql 查询时间" + Duration.between(LocalDateTime.now(), start).toString());
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<String> columnName = new ArrayList<>();
        for (int j = 1; j < resultSetMetaData.getColumnCount(); j++) {
            columnName.add(resultSetMetaData.getColumnLabel(j));
        }
        List<LinkedHashMap<String, String>> columnDataList = new ArrayList<>();
        List<List<String>> dataList = new ArrayList<>();
        while (resultSet.next()) {
            LinkedHashMap<String, String> columnMap = new LinkedHashMap<>();
            List<String> data = new ArrayList<>();
            for (String column : columnName) {
                String colVal = resultSet.getString(column);
                columnMap.put(column, colVal);
                data.add(colVal);
            }
            columnDataList.add(columnMap);
            dataList.add(data);
        }
        release(conn, statement, resultSet);
        ColumnData columnData = new ColumnData();
        columnData.setColumnName(columnName);
        columnData.setColumnDataList(columnDataList);
        columnData.setDataList(dataList);
        log.info("vertica sql 执行时间：" + Duration.between(LocalDateTime.now(), start).toString());
        return columnData;
    }


    public static Connection getConnection() {
        return jdbcPool.getConnection();
    }

    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if (st != null) {
            try {
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                jdbcPool.returnToQueue(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
