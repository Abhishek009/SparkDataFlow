package com.spark.dataflow.utils

import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import java.util.Calendar

object HS2 {

  def createDbStructure(conn: Connection,database:String,tablename:String ): Unit = {
    val sql = s"""
      create schema if not exists ${database};
      set schema ${database};
      create table if not exists ${database}.${tablename} (
        id int auto_increment primary key,
        db_url varchar(255) not null,
        table_name varchar(255) not null,
        dtm timestamp not null);"""
    val stmt = conn.createStatement()
    try {
      stmt.execute(sql)
    } finally {
      stmt.close()
    }
  }

  private def insertDbData(conn: Connection,database:String,tablename:String ): Unit = {
    val sqlIns =
      s"""insert into ${database}.${tablename}(db_url, table_name, dtm)
                values (?, ?, ?)"""
    val stmt = conn.prepareStatement(sqlIns)
    stmt.setString(1, conn.getMetaData.getURL)
    stmt.setString(2, s"${database}.${tablename}")
    stmt.setTimestamp(
      3,
      new Timestamp(Calendar.getInstance().getTime.getTime)
    )
    stmt.executeUpdate()
    stmt.close()
  }

  private def selectDbData(conn: Connection,database:String,tablename:String ): Unit = {
    val sqlIns =
      s"""select db_url, table_name, dtm from ${database}.${tablename}"""
    val stmt = conn.prepareStatement(sqlIns)
    val res: ResultSet = stmt.executeQuery()
    while ({
      res.next
    }) {
      val db_url = res.getString("db_url")
      val table_name = res.getString("table_name")
      val dtm = res.getTimestamp("dtm")
      println(db_url + " " + table_name + " " + dtm.toString)
    }
    stmt.close()
  }
  private def deleteDbData(conn: Connection, id: Int,database:String,tablename:String ): Unit = {
    val sqlDel = s"delete from ${database}.${tablename} where id = " + id.toString
    val stmt = conn.prepareStatement(sqlDel)
    stmt.execute()
    stmt.close()
  }
  def main(args: Array[String]): Unit = {

    Class.forName("org.h2.Driver")
    val conn: Connection =
      DriverManager.getConnection("jdbc:h2:./db/h2", "sa", "")
    try {
      createDbStructure(conn,"sparkdataflow","flowmanager")
      println("createDbStructure: done!")
      insertDbData(conn,"sparkdataflow","flowmanager")
      println("insertDbData: done!")
      selectDbData(conn,"sparkdataflow","flowmanager")
      //deleteDbData(conn, 2)
    } finally {
      conn.close()
    }
  }
}