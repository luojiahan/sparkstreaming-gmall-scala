package com.cuiyf41.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import com.alibaba.fastjson.JSONObject

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 从MySQL中查询数据的工具类
 */
object MySQLUtil {
  private val prop: Properties = MyPropertiesUtil.load("config.properties")
  private val dbDriver: String = prop.getProperty("db.gmall.rs.driver")
  private val dbUrl: String = prop.getProperty("db.gmall.rs.url")
  private val dbUser: String = prop.getProperty("db.gmall.rs.user")
  private val dbPassword: String = prop.getProperty("db.gmall.rs.password")

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from offset")
    println(list)
  }

  /**
   * 查询sql语句，返回业务结果
   * @param sql: String
   * @return rsList.toList: List[JSONObject]
   */
  def queryList(sql: String): List[JSONObject] ={
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName(dbDriver)
    //建立连接
    val conn: Connection = DriverManager.getConnection(
      dbUrl,
      dbUser,
      dbPassword)

    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val userStatusJsonObj = new JSONObject()
      for(i <-1 to rsMetaData.getColumnCount){
        userStatusJsonObj.put(rsMetaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    //释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }
}