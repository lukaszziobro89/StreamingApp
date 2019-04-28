package snowTickets

import com.google.gson.Gson
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import notifications.{SlackUtils, Telegram}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.json4s.JsonInput
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}


object ProcessKafka {

  def parseJson(jsonObject: JsonInput, jsonField: String): String = {
    val parsedJson = parse(jsonObject)
    compact(render(parsedJson \ jsonField))
  }

  def parseJsonPretty(jsonObject: JsonInput, jsonField: String): String = {
    val parsedJson = parse(jsonObject)
    pretty(render(parsedJson \ jsonField))
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

  def convertToJsonAllFields(message: JiraMessageFromJson): String= {
    val gson = new Gson
    val jsonString = gson.toJson(message)
    jsonString
  }

  def saveToElasticsearchOneIndex(message: String): Unit = {
    val client = ElasticClient(ElasticProperties("http://localhost:9200"))
    var indexName = "all_incidents_index"
    client.execute {
      indexInto(indexName / "_doc").doc(message)
        .refresh(RefreshPolicy.IMMEDIATE)
    }.await
    client.close()
  }

  def saveToElasticsearchMultipleIndexes(message: String): Unit = {
    val client = ElasticClient(ElasticProperties("http://localhost:9200"))
    var indexNameES = "unknown_index"
    if(message.contains("AIMS")) indexNameES = "aims_index"
    if(message.contains("MGA")) indexNameES = "mga_index"
    if(message.contains("Acturis")) indexNameES = "acturis_index"
    if(message.contains("Brokasure")) indexNameES = "brokasure_index"
    client.execute {
      indexInto(indexNameES / "_doc").doc(message)
        .refresh(RefreshPolicy.IMMEDIATE)
    }.await
    client.close()
  }

  def countProjects(message: RDD[String]) : Map[String, Int] = {
    val projectsCounts = message
      .map(inc => parseJsonPretty(inc, "project"))
      .map(project => (project, 1))
      .reduceByKey((p1, p2) => p1 + p2)
      .collect()
    projectsCounts.toMap
  }

  def formatNotificationSlack(message: Map[String, Int]) : String = {
    val notification = new StringBuilder("")
    notification.append("Number of incidents created:\n")
    for ((project, numberOfIncidents) <- message){
      notification.append(s"$project - $numberOfIncidents" + "\n")
    }
    notification.toString()
  }

  def formatNotificationTelegram(message: Map[String, Int]) : String = {
    val notification = new StringBuilder("")
    notification.append("Number of incidents created:" + "%0D%0A")
    for ((project, numberOfIncidents) <- message){
      notification.append(s"$project - $numberOfIncidents" + "%0D%0A")
    }
    notification.toString()
  }

  /**
    * Main method which processes each message
    * @param message Input RDD message received from Kafka topic
    */
  def processKafkaMessage(message: RDD[ConsumerRecord[String, String]]) : Unit ={
    if (!message.isEmpty()) {

      println("---------------------------")
      println("Processing received RDD ...")

      val incidentsCountForProject = countProjects(message.map(x => x.value()))
      val stringMessageSlack = formatNotificationSlack(incidentsCountForProject)
      val stringMessageTelegram = formatNotificationTelegram(incidentsCountForProject)

      Telegram.sendNotification(stringMessageTelegram.toString)
      SlackUtils.sendMessageToChannel("grafana_luq89", stringMessageSlack)

      message
        .map(inc => inc.value())
        .map(inc => parseFieldsStringAndStringJson(inc))
        .map(inc => JiraMessageFromJson.tupled(inc))
        .map(convertToJsonAllFields)
        .foreach(inc => saveToElasticsearchOneIndex(inc))

      println("RDD processing completed!")
      println("---------------------------")

    }else{
      println("Empty message")
    }
  }
}

case class JiraMessageFromJson(
                                statusName: String,
                                displayName: String,
                                emailAddress: String,
                                created: Double,
                                updated: Double,
                                project: String
                              )
