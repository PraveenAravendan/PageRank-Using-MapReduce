package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Spark/spark-2.4.6-bin-hadoop2.7")
    
    if (args.length != 3) {
      println("Usage (User parameters order): PageRank -> InputDir iterations OutputDir")
    }
    
    // Input directory (File Path), Number of iterations for the Page Rank Algorithm, Output directory (File Path)
    val inputFilePath = args(0); val iterations = args(1).toInt; val outputFilePath = args(2)
   
    // Creating sparkContext and sqlContext
    val sparkConf = new SparkConf().setAppName("Page Rank").setMaster("local").set("spark.driver.host", "localhost")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import sqlContext.implicits._
    
    //DataFrame from csv file
    val inputData = sqlContext.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter", ",").load(inputFilePath)
    //Dataframe consists of only Origin airport and Destination airport
    val airportData = inputData.select("ORIGIN", "DEST")
    
    val airportRoutes = airportData.map(row => (row(0).toString(), row(1).toString())).collect()
    val airportRoutesParallel = sparkContext.parallelize(airportRoutes)
    // Link of one airport to (list of airports) that it is connected to
    val airportGroupedLinks = airportRoutesParallel.groupByKey()
    
    // Give PageRank of value 1 to every node
    var pageRanks = airportGroupedLinks.mapValues(v => 10.0)    
    
    // Running for n iterations
    for (i <- 1 to iterations) {
      val airportMaps = airportGroupedLinks.join(pageRanks).values.flatMap{ case (airports, pageRank) =>
        val div = pageRank/airports.size
        airports.map(airportNode => (airportNode, div))
      }
      val airportNodeCount = airportGroupedLinks.count()
      pageRanks = airportMaps.reduceByKey(_ + _).mapValues((0.15/airportNodeCount) + 0.85 * _)
    }

    // Sorting the pageRanks result in descending order
    val outputResult = pageRanks.sortBy(_._2, false)
    
    //outputResult.collect().foreach(println)
    
    // Writing to output directory (File Path)
    outputResult.saveAsTextFile(outputFilePath)
  }
}