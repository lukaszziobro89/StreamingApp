package other

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, sum}

object DataFrames {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark
    .read
    .format("csv")
    .option("header", "true")
    .load("/home/luq/Downloads/FIFA19Players.csv")
    .cache()

  df.printSchema()
  df.toDF().show(10)
  df.select("Player Name").show(10)
  df.filter("Age < 20").show()
  df.filter(df("Nationality")==="England").show(20)

  val countriesCount = df.select("Nationality").distinct().count()

  df
    .groupBy("Nationality")
    .agg(count("Nationality").name("PlayersCounter"))
    .orderBy(desc("PlayersCounter"))
    .show()

  df
    .groupBy("nationality")
    .agg(sum("Overall").name("Total Overall"))
    .orderBy(desc("Total Overall"))
    .show()

  val x = df
    .groupBy("nationality")
    .agg(sum("Overall")
    .name("Total Overall"))
    .orderBy(desc("Total Overall"))

  x.coalesce(1).write.csv("/home/luq/Desktop/overall")


}
