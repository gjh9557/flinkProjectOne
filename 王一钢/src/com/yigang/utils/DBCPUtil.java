package com.yigang.utils;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Time：2019-12-21 10:46
 * Email： yiie315@163.com
 * Desc：DBCP数据库连接池工具类
 *
 * @author： 王一钢
 * @version：1.0.0
 */
public class DBCPUtil {
    private static DataSource pool;

    static {
        try {
            Properties properties = new Properties();
            properties.load(DBCPUtils.class.getClassLoader().getResourceAsStream("dbcp.properties"));
            pool = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("不好！连接池获取失败了哦！...");
        }
    }


    /**
     * 获得连接的实例
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return pool.getConnection();
    }


    /**
     * 返回连接池的实例
     *
     * @return
     */
    public static DataSource getConnectionPool() {
        return pool;
    }
}
