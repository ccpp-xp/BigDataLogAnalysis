package com.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtil {

  val password = "Zhaotianxiang74#"
  val username = "root"
  val url = "jdbc:mysql://101.200.121.97:3306/bigdata?useSSL=false"

  def getConn(): Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(url, username, password)
  }


  def insertTable1(t: (String, String, String, Int, Int)) {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val sql = "insert into table1(remote_addr,remote_user,time_local,status,body_bytes_send) values (?,?,?,?,?)"
    val conn = DriverManager.getConnection(url, username, password)
    val ps: PreparedStatement = conn.prepareStatement(sql)
    ps.setString(1, t._1.trim)
    ps.setString(2, t._2.trim)
    ps.setString(3, t._3.trim)
    ps.setInt(4, t._4)
    ps.setInt(5, t._5)
    ps.executeUpdate()
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }

  def insertTable2(t: (String, String, String, String, String)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val sql = "insert into table2(request_method,request_url,http_version,http_referer,http_user_agent) values (?,?,?,?,?)"
    val conn = DriverManager.getConnection(url, username, password)
    val ps: PreparedStatement = conn.prepareStatement(sql)
    ps.setString(1, t._1.trim)
    ps.setString(2, t._2.trim)
    ps.setString(3, t._3.trim)
    ps.setString(4, t._4.trim)
    ps.setString(5, t._5.trim)
    ps.executeUpdate()
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }

  def insertTableLog(t: (String, String, String, String, String,String, String, String, Int, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val sql = "insert into table2(request_method,request_url,http_version,http_referer,http_user_agent," +
      "remote_addr,remote_user,time_local,status,body_bytes_send) values (?,?,?,?,?,?,?,?,?,?)"
    val conn = DriverManager.getConnection(url, username, password)
    val ps: PreparedStatement = conn.prepareStatement(sql)
    ps.setString(1, t._1.trim)
    ps.setString(2, t._2.trim)
    ps.setString(3, t._3.trim)
    ps.setString(4, t._4.trim)
    ps.setString(5, t._5.trim)
    ps.setString(6, t._6.trim)
    ps.setString(7, t._7.trim)
    ps.setString(8, t._8.trim)
    ps.setInt(9, t._9)
    ps.setInt(10, t._10)
    ps.executeUpdate()
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }


  def hourCount(t: (Int, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "hour_count"
    val idName = "hour"
    val valueName = "count"
    val id = t._1
    val value = t._2

    val conn = DriverManager.getConnection(url, username, password)
    try {
      // 检查id是否存在于表中
      val selectQuery = s"SELECT * FROM $tableName WHERE $idName = $id"
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery(selectQuery)

      if (resultSet.next()) {
        // 如果id存在，执行累加value操作
        val currentValue = resultSet.getInt(s"$valueName")
        val updatedValue = currentValue + value

        val updateQuery = s"UPDATE $tableName SET $valueName = $updatedValue WHERE $idName = $id"
        statement.executeUpdate(updateQuery)
      } else {
        // 如果id不存在，执行插入操作
        val insertQuery = s"INSERT INTO $tableName ($idName, $valueName) VALUES ($id, $value)"
        statement.executeUpdate(insertQuery)
      }
    } finally {
      // 关闭连接
      conn.close()
    }
  }

  def ipCount(t: (String, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "ip_count"
    val idName = "ip"
    val valueName = "count"
    val id = t._1.trim
    val value = t._2
    val conn = DriverManager.getConnection(url, username, password)
    try {
      // 检查id是否存在于表中
      val selectQuery = s"SELECT * FROM $tableName WHERE $idName = $id"
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery(selectQuery)

      if (resultSet.next()) {
        // 如果id存在，执行累加value操作
        val currentValue = resultSet.getInt(s"$valueName")
        val updatedValue = currentValue + value

        val updateQuery = s"UPDATE $tableName SET $valueName = $updatedValue WHERE $idName = $id"
        statement.executeUpdate(updateQuery)
      } else {
        // 如果id不存在，执行插入操作
        val insertQuery = s"INSERT INTO $tableName ($idName, $valueName) VALUES ($id, $value)"
        statement.executeUpdate(insertQuery)
      }
    } finally {
      // 关闭连接
      conn.close()
    }
  }

  def ipCountOneDay(t: (String,Long, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "ip_count_oneDay"
    val idName = "ip"
    val timeName = "time"
    val valueName = "count"
    val id = t._1.trim
    val time = t._2
    val count = t._3
    val conn = DriverManager.getConnection(url, username, password)
    try {
      val statement = conn.createStatement()
        // 插入数据
        val insertQuery = s"INSERT INTO $tableName ($idName, $timeName, $valueName) VALUES ($id, $time, $count)"
        statement.executeUpdate(insertQuery)
    } finally {
      // 关闭连接
      conn.close()
    }
  }

  def statusInsert(t:(String, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "urlStatus"
    val statusName = "status"
    val valueName = "value"
    val id = t._1.trim
    val value = t._2
    val conn = DriverManager.getConnection(url, username, password)
    try {
      val statement = conn.createStatement()
      // 插入数据
      val insertQuery = s"INSERT INTO $tableName ($statusName,  $valueName) VALUES ($id, $value)"
      statement.executeUpdate(insertQuery)
    } finally {
      // 关闭连接
      conn.close()
    }
  }

  def agentInsert(t:(String, Int)): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val tableName = "UserAgent"
    val statusName = "agent"
    val valueName = "num"
    val id = t._1.trim
    val value = t._2
    val conn = DriverManager.getConnection(url, username, password)
    try {
      val statement = conn.createStatement()
      // 插入数据
      val insertQuery = s"INSERT INTO $tableName ($statusName,  $valueName) VALUES ($id, $value)"
      statement.executeUpdate(insertQuery)
    } finally {
      // 关闭连接
      conn.close()
    }
  }

}
