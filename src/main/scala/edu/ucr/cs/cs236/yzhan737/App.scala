package edu.ucr.cs.cs236.yzhan737

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

/**
 * @author Yaming Zhang
 */
object App {

  def main(args: Array[String]) {
    // parse the 3 input folders
    val locations = args(0)
    val recordings = args(1)
    val output = args(2)

    // set up the spark configuration
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    // create the spark session for DataFrame API
    val spark = SparkSession
      .builder()
      .appName("CS236_class_project")
      .config(conf)
      .getOrCreate()

    try {
      val t1 = System.nanoTime
      import spark.implicits._
      // Load the .csv file. Separate with "," and skip the header
      val inputCSV = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(locations + "/WeatherStationLocations.csv")

      // show the connection between the USAF and STATE
      val dataFrameCSV = inputCSV
        // we only need 3 attributes in the file
        .select($"USAF", $"CTRY", $"STATE")
        // select the stations in the US
        .filter($"CTRY" === "US" && $"STATE" =!= "")
        // all the recording now are in the US
        .select($"USAF", $"STATE")

      // load the .txt file during 4 years
      val year2006 = spark.read.textFile(recordings + "/2006.txt")
      val year2007 = spark.read.textFile(recordings + "/2007.txt")
      val year2008 = spark.read.textFile(recordings + "/2008.txt")
      val year2009 = spark.read.textFile(recordings + "/2009.txt")

      // calculate the precipitation in year 2006
      val dataFrame2006 = year2006
        // filter the headers of the file
        .filter(s => !s.startsWith("STN---"))
        // filter the precipitation of the number 99.99, which means that there is no data
        .filter(s => s.charAt(118) != '9')
        // map the file using USAF and the month of the record
        // for the precipitation, we convert the record into double via the letter at the end
        .map(s =>
          if (s.substring(119, 124).endsWith("A")) {
            (s.substring(0, 6), s.substring(18, 20), 4 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("B") || s.substring(119, 124).endsWith("E")) {
            (s.substring(0, 6), s.substring(18, 20), 2 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("C")) {
            (s.substring(0, 6), s.substring(18, 20), 8 / 6 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("D") || s.substring(119, 124).endsWith("F") || s.substring(119, 124).endsWith("G")) {
            (s.substring(0, 6), s.substring(18, 20), s.substring(119, 123).toDouble)
          } else {
            (s.substring(0, 6), s.substring(18, 20), 0.0)
          }
        )
        // group all the recordings via "USAF" and "Month"
        .groupBy($"_1", $"_2")
        // calculate the sum of the precipitation of each station during each month in 2006
        .sum()
        // rename the column for later union function
        .withColumnRenamed("_1", "USAF")
        .withColumnRenamed("_2", "month")
        .withColumnRenamed("sum(_3)", "PRCP")

      // calculate the precipitation in year 2007
      val dataFrame2007 = year2007
        // filter the headers of the file
        .filter(s => !s.startsWith("STN---"))
        // filter the precipitation of the number 99.99, which means that there is no data
        .filter(s => s.charAt(118) != '9')
        // map the file using USAF and the month of the record
        // for the precipitation, we convert the record into double via the letter at the end
        .map(s =>
          if (s.substring(119, 124).endsWith("A")) {
            (s.substring(0, 6), s.substring(18, 20), 4 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("B") || s.substring(119, 124).endsWith("E")) {
            (s.substring(0, 6), s.substring(18, 20), 2 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("C")) {
            (s.substring(0, 6), s.substring(18, 20), 8 / 6 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("D") || s.substring(119, 124).endsWith("F") || s.substring(119, 124).endsWith("G")) {
            (s.substring(0, 6), s.substring(18, 20), s.substring(119, 123).toDouble)
          } else {
            (s.substring(0, 6), s.substring(18, 20), 0.0)
          }
        )
        // group all the recordings via Tuple2("USAF", "Month")
        .groupBy($"_1", $"_2")
        // calculate the sum of the precipitation of each station during each month in 2006
        .sum()
        // rename the column for later union function
        .withColumnRenamed("_1", "USAF")
        .withColumnRenamed("_2", "month")
        .withColumnRenamed("sum(_3)", "PRCP")

      // calculate the precipitation in year 2008
      val dataFrame2008 = year2008
        // filter the headers of the file
        .filter(s => !s.startsWith("STN---"))
        // filter the precipitation of the number 99.99, which means that there is no data
        .filter(s => s.charAt(118) != '9')
        // map the file using USAF and the month of the record
        // for the precipitation, we convert the record into double via the letter at the end
        .map(s =>
          if (s.substring(119, 124).endsWith("A")) {
            (s.substring(0, 6), s.substring(18, 20), 4 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("B") || s.substring(119, 124).endsWith("E")) {
            (s.substring(0, 6), s.substring(18, 20), 2 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("C")) {
            (s.substring(0, 6), s.substring(18, 20), 8 / 6 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("D") || s.substring(119, 124).endsWith("F") || s.substring(119, 124).endsWith("G")) {
            (s.substring(0, 6), s.substring(18, 20), s.substring(119, 123).toDouble)
          } else {
            (s.substring(0, 6), s.substring(18, 20), 0.0)
          }
        )
        // group all the recordings via Tuple2("USAF", "Month")
        .groupBy($"_1", $"_2")
        // calculate the sum of the precipitation of each station during each month in 2006
        .sum()
        // rename the column for later union function
        .withColumnRenamed("_1", "USAF")
        .withColumnRenamed("_2", "month")
        .withColumnRenamed("sum(_3)", "PRCP")

      // calculate the precipitation in year 2009
      val dataFrame2009 = year2009
        // filter the headers of the file
        .filter(s => !s.startsWith("STN---"))
        // filter the precipitation of the number 99.99, which means that there is no data
        .filter(s => s.charAt(118) != '9')
        // map the file using USAF and the month of the record
        // for the precipitation, we convert the record into double via the letter at the end
        .map(s =>
          if (s.substring(119, 124).endsWith("A")) {
            (s.substring(0, 6), s.substring(18, 20), 4 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("B") || s.substring(119, 124).endsWith("E")) {
            (s.substring(0, 6), s.substring(18, 20), 2 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("C")) {
            (s.substring(0, 6), s.substring(18, 20), 8 / 6 * s.substring(119, 123).toDouble)
          } else if (s.substring(119, 124).endsWith("D") || s.substring(119, 124).endsWith("F") || s.substring(119, 124).endsWith("G")) {
            (s.substring(0, 6), s.substring(18, 20), s.substring(119, 123).toDouble)
          } else {
            (s.substring(0, 6), s.substring(18, 20), 0.0)
          }
        )
        // group all the recordings via Tuple2("USAF", "Month")
        .groupBy($"_1", $"_2")
        // calculate the sum of the precipitation of each station during each month in 2006
        .sum()
        // rename the column for later union function
        .withColumnRenamed("_1", "USAF")
        .withColumnRenamed("_2", "month")
        .withColumnRenamed("sum(_3)", "PRCP")

      // union all the recordings during the 4 years for calculation of the average precipitation
      val precipitation = dataFrame2006
        .union(dataFrame2007)
        .union(dataFrame2008)
        .union(dataFrame2009)
        // group by station and month and calculate the average precipitation
        .groupBy($"USAF", $"month")
        .avg("PRCP")
        .withColumnRenamed("avg(PRCP)", "PRCP")

      // join two tables via USAF to see which state does the station belong to
      val stateData = dataFrameCSV
        // join two tables via USAF
        .join(precipitation, Seq("USAF"))
        // add precipitation of all the stations in the state
        .groupBy($"STATE", $"month")
        .sum("PRCP")
        .withColumnRenamed("sum(PRCP)", "PRCP")
        // we group all the data via state so that we can calculate the maximum and minimum precipitation of the state
        .groupBy($"STATE")

      val max = stateData
        // calculate the maximum precipitation of the state
        .max("PRCP")
        .withColumnRenamed("max(PRCP)", "Max")
        // Round the maximum value and keep two decimal places
        .selectExpr("STATE", "cast(Max as decimal(38,2)) as Max")

      val min = stateData
        // calculate the minimum precipitation of the state
        .min("PRCP")
        .withColumnRenamed("min(PRCP)", "Min")
        // Round the minimum value and keep two decimal places
        .selectExpr("STATE", "cast(Min as decimal(38,2)) as Min")

      import org.apache.spark.sql.functions.{col, udf}
      // define a function to calculate the difference of the max and min value
      val diffUDF = udf((max: Double, min: Double) => (max - min).formatted("%.2f").toDouble)

      val result = max
        // join the max and min tables to calculate the difference
        .join(min, Seq("STATE"))
        // calculate the difference using the UDF
        .withColumn("Diff", diffUDF(col("Max"), col("Min")))
        // the most important part: order via the difference
        .orderBy("Diff")

      // write the final result to the output directory
      result.coalesce(1).write.option("header", "true").csv(output)

      val t2 = System.nanoTime
      println(s"SUCCESS!\nThe result has been written to the output directory\nOperation finished in ${((t2 - t1) * 1E-9)} seconds")
    } finally {
      spark.stop
    }
  }
}