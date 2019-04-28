package snowTickets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main extends App{

  // Spark streaming context configuration
  val conf = new SparkConf().setMaster("local[*]").setAppName("Incidents")
  conf.set("spark.streaming.ui.retainedBatches", "5")
  conf.set("spark.streaming.backpressure.enabled", "true")
  conf.set("bootstrap.servers","localhost:9092")

  val ssc = new StreamingContext(conf, batchDuration = Seconds(5))

  // Logging
  Logger.getLogger("org.apache.spark.streaming").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.streaming.dstream.DStream").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.streaming.dstream.WindowedDStream").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.streaming.DStreamGraph").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.streaming.scheduler.JobGenerator").setLevel(Level.ERROR)


  // Kafka configuration
  val kafkaParams = Map(
    "metadata.broker.list" -> "localhost:9092",
    "bootstrap.servers" -> "localhost:9092",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "test")

  // create streaming connect to Kafka topic
  val kafkaTopics = Set("all_incidents")
  val messages = KafkaUtils.createDirectStream(ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaParams)
  )

  // process each RDD
  messages.foreachRDD(
    kafkaMessage => snowTickets.ProcessKafka.processKafkaMessage(kafkaMessage)
  )

  // start streaming computation
  ssc.start
  ssc.awaitTermination()

}
