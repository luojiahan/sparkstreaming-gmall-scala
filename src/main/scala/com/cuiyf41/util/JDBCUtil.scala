package com.cuiyf41.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {
  //初始化连接池
  var dataSource: DataSource = init()

  /**
   * 初始化连接池方法
   * @param
   * @return dataSource: DataSource
   */
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hdp103:3306/gmall?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "199037")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
   * 获取JDBC连接
   * @param
   * @return dataSource.getConnection
   */
  def getConnection: Connection = {
    dataSource.getConnection
  }


  /**
   * 执行SQL语句,单条数据插入
   * @param connection: Connection
   * @param sql: String, eg：“select * from user_ad_count where dt = ? and userid = ? and adid = ?”
   * @param params: Array[Any], eg：Array(day, user, ad)对应sql中第一、二、三个params的数值
   * @return rtn: Int
   */
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }
  /**
   * 执行SQL语句,判断数据是否存在
   * @param connection: Connection
   * @param sql: String, eg：“select * from user_ad_count where dt = ? and userid = ? and adid = ?”
   * @param params: Array[Any], eg：Array(day, user, ad)对应sql中第一、二、三个params的数值
   * @return flag: Boolean
   */
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }
}
