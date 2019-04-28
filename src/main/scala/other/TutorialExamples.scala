package other

import com.google.gson.Gson
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import notifications.Telegram
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.JsonInput
import org.json4s.jackson.JsonMethods._

object TutorialExamples {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.sparkContext.setLogLevel("ERROR")

  val incidents = sc.textFile("/home/luq/Desktop/JsonIncidents.txt")

  println(incidents.getClass)

  def parseFieldsStringAndFloat(incident: String, stringFieldIndex: Int, floatFieldIndex: Int) : (String, Float) = {
    val fields = incident.split(",")
    val textField = fields(stringFieldIndex).toLowerCase
    val floatField = fields(floatFieldIndex).toFloat
    (textField, floatField )
  }

  def parseFieldsStringAndDouble(incident: String, stringFieldIndex: Int, doubleFieldIndex: Int) : (String, Double) = {
    val fields = incident.split(",")
    val textField = fields(stringFieldIndex).toLowerCase
    val doubleField = fields(doubleFieldIndex).toDouble
    (textField, doubleField)
  }

  def parseFieldsStringAndInt(incident: String, stringFieldIndex: Int, intFieldIndex: Int) : (String, Int) = {
    val fields = incident.split(",")
    val textField = fields(stringFieldIndex).toLowerCase
    val intField = fields(intFieldIndex).toInt
    (textField, intField)
  }

  def parseFieldsStringAndString(incident: String, stringFieldIndex1: Int, stringFieldIndex2: Int): (String, String) = {
    val fields = incident.split(",")
    val textField1 = fields(stringFieldIndex1).toLowerCase
    val textField2 = fields(stringFieldIndex2).toLowerCase
    (textField1, textField2)
  }

  def getStatusAndProject(incident: String): (String, String) = {
    val fields = incident.split(",")
    val status = fields(0).toLowerCase
    val projectField = fields(5)
    val message = (status, projectField)
    message
  }

  def filterByProject(project: String):  RDD[(String, String)] = {
    val projectIncidents = incidents
      .filter(inc => inc.toLowerCase.contains(project.toLowerCase))
      .map(getStatusAndProject)
    projectIncidents
  }

  def convertToJsonStringDouble(message: JiraMessageStringDouble)= {
    val gson = new Gson
    val jsonString = gson.toJson(message)
    jsonString
  }

  def convertToJsonStringString(message: JiraMessageStringString)= {
    val gson = new Gson
    val jsonString = gson.toJson(message)
    jsonString
  }

  def parseJson(jsonObject: JsonInput, jsonField: String): String = {
    val parsedJson = parse(jsonObject)
    compact(render(parsedJson \ jsonField))
  }

  def parseJsonPretty(jsonObject: JsonInput, jsonField: String): String = {
    val parsedJson = parse(jsonObject)
    pretty(render(parsedJson \ jsonField))
  }

 def saveToElasticsearch(message: String, indexName: String): Unit = {
   val client = ElasticClient(ElasticProperties("http://localhost:9200"))
     client.execute {
       indexInto(indexName / "_doc").doc(message)
         .refresh(RefreshPolicy.IMMEDIATE)
     }.await
   client.close()
 }

// def processMessage(incidents: RDD[String], searchExpression: String) : Unit ={
//  incidents
//    .filter(inc => inc.contains(searchExpression))
//    .map(inc => parseFieldsStringAndDouble(inc, 1, 3))
//    .map(message => JiraMessageStringDouble tupled message)
//    .map(convertToJsonStringDouble)
//    .foreach(inc => saveToElasticsearch(inc))
// }

  def convertToJsonAllFields(message: JiraMessageFromJson): String= {
    val gson = new Gson
    val jsonString = gson.toJson(message)
    jsonString
  }

  def processKafkaMessage(message: RDD[ConsumerRecord[String, String]]) : Unit ={
    if (!message.isEmpty()) {

    //  TODO: add Elasticsearach client to be opened only once per non-empty RDD

      message
        .map(inc => inc.value())
        .map(inc => parseFieldsStringAndStringJson(inc))
        .map(inc => JiraMessageFromJson.tupled(inc))
        .map(convertToJsonAllFields)
        .foreach(saveToElasticsearchCase)

//      val indexName = messageString match {
//        case messageString if messageString.toLowerCase.contains("aims")   => "aims_index";
//        case x if x.toLowerCase.contains("mga")   => "mga_index";
//        case messageString if messageString.toLowerCase.contains("acturis") => "acturis_index"
//        case messageString if messageString.toLowerCase.contains("brokasure") => "brokasure_index"
//        case _                             => "unknown_index";
//      }

//      x.foreach(inc => saveToElasticsearchCase(inc))

    }else{
      println("Empty message")
    }
  }

  def saveToElasticsearchCase(message: String): Unit = {
    val client = ElasticClient(ElasticProperties("http://localhost:9200"))

    var indexName = "unknown_index"
    if(message.contains("AIMS")) indexName = "aims_index"
    if(message.contains("MGA")) indexName = "mga_index"
    if(message.contains("Acturis")) indexName = "acturis_index"
    if(message.contains("Brokasure")) indexName = "brokasure_index"

    client.execute {
      indexInto(indexName / "_doc").doc(message)
        .refresh(RefreshPolicy.IMMEDIATE)
    }.await
    client.close()
  }

  def parseFieldsStringAndStringJson(incident: String): (String, String, String, Double, Double, String) = {
    val statusName = parseJson(incident, "statusName").replaceAll("\"", "")
    val displayName = parseJson(incident, "displayName").replaceAll("\"", "")
    val emailAddress = parseJson(incident, "emailAddress").replaceAll("\"", "")
    val created = parseJson(incident, "created").toDouble
    val updated = parseJson(incident, "updated").toDouble
    val project = parseJson(incident, "project").replaceAll("\"", "")

    (statusName, displayName, emailAddress, created, updated, project)
  }

  def processKafkaMessageJson(searchExpression: String, returnJsonField: String): Unit={
    incidents
      .filter(message => message.toLowerCase().contains(searchExpression.toLowerCase))
      .map(message => parseJson(message, returnJsonField))
      .foreach(println)
  }

  val AIMS_project = "AIMS"
  val Brokasure_project = "bRoKaSuRe"

  incidents.foreach(println)

  def getProject(incident: String): String = {
    val fields = incident.split(",")
    val projectField = fields(6)
    projectField
  }

  def countProjects(message: RDD[String]) : Map[String, Int] = {
    val projectsCounts = message
        .map(inc => parseJsonPretty(inc, "project"))
        .map(project => (project, 1))
        .reduceByKey((p1, p2) => p1 + p2)
        .collect()
    projectsCounts.toMap
  }

  def formatNotification(message: Map[String, Int]) : String = {
    val notification = new StringBuilder("")
    notification.append("Number of incidents created:" + System.lineSeparator())
      for ((project, numberOfIncidents) <- x){
        notification.append(s"$project - $numberOfIncidents" + System.lineSeparator())
      }
    notification.toString()
  }

  val x = countProjects(incidents)
  println(formatNotification(x))

  val notificationTest = formatNotification(x)

  Telegram.sendNotification("Ticket created!")
  Telegram.sendNotification(notificationTest)



//  x.foreach(inc => println(inc))

//  processKafkaMessageJson("AIMS", "statusName")

//  processMessage(incidents, "Brokasure")

}

case class JiraMessageStringString(status: String, project: String)
case class JiraMessageStringDouble(status: String, created: Double)
case class JiraMessageFromJson(
                                statusName: String,
                                displayName: String,
                                emailAddress: String,
                                created: Double,
                                updated: Double,
                                project: String
                              )
