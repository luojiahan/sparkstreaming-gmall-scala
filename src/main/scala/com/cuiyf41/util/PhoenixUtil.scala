package com.cuiyf41.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import com.alibaba.fastjson.JSONObject

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * Desc:  用于从Phoenix中查询数据
 * User_id     if_consumerd
 *   zs            1
 *   ls            1
 *   ww            1
 *
 *  期望结果：
 *  {"user_id":"zs","if_consumerd":"1"}
 *  {"user_id":"zs","if_consumerd":"1"}
 *  {"user_id":"zs","if_consumerd":"1"}
 *
 *  jdbc处理步骤
 *  1、注册驱动
 *  2、建立连接
 *  3、创建数据库操作对象
 *  4、执行SQL语句
 *  5、处理结果集
 *  6、释放资源
 */
object PhoenixUtil {
  private val prop: Properties = MyPropertiesUtil.load("config.properties")
  private val phoenixUrl: String = prop.getProperty("jdbc.phoenix.url")
  private val phoenixUser: String = prop.getProperty("jdbc.phoenix.user")
  private val phoenixPassword: String = prop.getProperty("jdbc.phoenix.password")

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status")
    println(list)
  }

  /**
   * 在Phoenix中查询sql，获取结果集
   * @param sql:String
   * @return rsList.toList: List[JSONObject]
   */
  def queryList(sql:String): List[JSONObject] ={
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //建立连接
    val conn: Connection = DriverManager.getConnection(phoenixUrl, phoenixUser, phoenixPassword)
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val userStatusJsonObj = new JSONObject()
      //{"user_id":"zs","if_consumerd":"1"}
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