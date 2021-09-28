package jsonparser

import net.liftweb.json.{DefaultFormats, parse}

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.io.Source

case class Language(name: String, createdBy: String, firstAppeared: Int, description: String)

object JsonParser extends App {
  val DbConfFilename = "db.properties"
  val JsonFilename = "programming_languages.json"
  implicit val formats = DefaultFormats

  val db = new Properties()
  db.load(Source.fromResource(DbConfFilename).bufferedReader())
  var conn: Connection = _
  try {
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn = DriverManager.getConnection(db.getProperty("url"), db.getProperty("user"), db.getProperty("password"))
    createTable(conn)
    parseJson(JsonFilename).foreach(insertData(conn, _))
    selectLanguages(conn).foreach(printLanguage)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def parseJson(filename: String): List[Language] = {
    val jsonFile = Source.fromResource(filename).mkString
    val json = parse(jsonFile)
    (json \\ "language").children.map(_.extract[Language])
  }

  def createTable(conn: Connection): Unit = {
    val st = conn.createStatement()
    st.executeUpdate("DROP TABLE IF EXISTS PROGRAMMING_LANGUAGES")
    st.executeUpdate(
      "CREATE TABLE PROGRAMMING_LANGUAGES" +
        "(NAME VARCHAR(50) NOT NULL," +
        "CREATED_BY VARCHAR(50) NOT NULL," +
        "FIRST_APPEARED INTEGER NOT NULL," +
        "DESCRIPTION VARCHAR(200) NOT NULL)")
  }

  def insertData(conn: Connection, language: Language): Unit = {
    val st = conn.createStatement()
    st.executeUpdate(
      "INSERT INTO PROGRAMMING_LANGUAGES VALUES(" +
        s"'${language.name}', '${language.createdBy}', ${language.firstAppeared}, '${language.description}')")
  }

  def selectLanguages(conn: Connection): ListBuffer[Language] = {
    val st = conn.createStatement()
    val rs = st.executeQuery("SELECT * FROM PROGRAMMING_LANGUAGES")
    val languages = new ListBuffer[Language]()
    while (rs.next) {
      languages += Language(rs.getString("NAME"), rs.getString("CREATED_BY"), rs.getInt("FIRST_APPEARED"), rs.getString("DESCRIPTION"))
    }
    languages
  }

  def printLanguage(language: Language): Unit =
    println(s"${language.name}, ${language.createdBy}, ${language.firstAppeared}\n  ${language.description}")
}
