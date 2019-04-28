package other

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSpark {


  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(2))
  val zkQuorum = "localhost:2181"
  val group = "test-group"
  val topics = "test"
  val numThreads = 1
  val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//  val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group,
//  val lines = lineMap.map(_._2)
//  val words = lines.flatMap(_.split(" "))
//  val pair = words.map( x => (x,1))
//  val wordCounts = pair.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//  wordCounts.print
//
//
//  ssc.start
//  ssc.awaitTermination


  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent from a starvation scenario.
/*
  val topics = "test"
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(5))

  val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9092)
  print(lines)
  // Split each line into words
  val words = lines.flatMap(_.split(" "))
  print(lines)
  // Count each word in each batch
  val pairs = words.map(word => (word, 1))
//  val wordCounts = pairs.reduceByKey(_ + _)

  val wordCounts = pairs.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()
  print(wordCounts)

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
*/
//============================================================================



}
