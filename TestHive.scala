/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

object TestHive {
  def printMemoryUsage(): Unit = {
    val rt = Runtime.getRuntime
    val total = rt.totalMemory()
    val free = rt.freeMemory()
    val used = total - free

    // scalastyle:off
    println("-------------------------------------")
    println("Total: " + total +
        ", Used: " + used +
        ", Free: " + free)
    println("-------------------------------------")
    // scalastyle:on
  }

  def main(args: Array[String]): Unit = {
    //      .config("spark.driver.allowMultipleContexts", "true")
//    val spark = SparkSession
//      .builder().master("local[1]")
//      .appName("interfacing spark sql to hive metastore without configuration file")
//      .config("hive.metastore.uris", "thrift://bomeng-master.fyre.ibm.com:9083") // replace with your hivemetastore service's thrift url
//      .enableHiveSupport() // don't forget to enable hive support
//      .getOrCreate()
//
//    import spark.implicits._
//    import spark.sql
//    // create an arbitrary frame
//    val frame = Seq(("one", 1), ("two", 2), ("three", 3)).toDF("word", "count")
//    // see the frame created
//    frame.show()
//    /**
//     * +-----+-----+
//     * | word|count|
//     * +-----+-----+
//     * |  one|    1|
//     * |  two|    2|
//     * |three|    3|
//     * +-----+-----+
//     */
//    // write the frame
//    frame.write.mode("overwrite").saveAsTable("t4")


//    val logFile = "/Users/mengbo/spark/README.md" // Should be some file on your system
//    val spark = SparkSession.builder.master("local[4]")
//      .config("hive.metastore.uris", "thrift://eraser-node-2.fyre.ibm.com:9083")
//      .enableHiveSupport().appName("Simple Application").getOrCreate()
//
//    import spark.implicits._
//    import spark.sql
//
//    val frame = Seq(("one", 1), ("two", 2), ("three", 3)).toDF("word", "count")
//    frame.show()
//
//    frame.write.mode("overwrite").saveAsTable("t4")

//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")

    printMemoryUsage()

    val conf1 = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("hive.metastore.uris", "thrift://therapy-node-3.fyre.ibm.com:9083")
      .set("spark.ui.port", "4041")
      .set("spark.sql.catalogImplementation", "hive")
    val sc1 = new SparkContext(conf1)
    val spark1 = new SparkSession(sc1)
    spark1.sql("show databases").show()
    spark1.sql("use bdp")
    spark1.sql("show tables").show()
    spark1.sql("select * from reports").show()

    printMemoryUsage()

    val conf2 = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("hive.metastore.uris", "thrift://hulks1.fyre.ibm.com:9083")
      .set("spark.ui.port", "4042")
      .set("spark.sql.catalogImplementation", "hive")
    val sc2 = new SparkContext(conf2)
//      val spark2 = SparkSession.builder().config(conf2).getOrCreate()
    val spark2 = new SparkSession(sc2)
    spark2.sql("show databases").show()
    spark2.sql("use test1")
    spark2.sql("show tables").show()
    spark2.sql("select * from reports").show()

    printMemoryUsage()

    spark1.stop()
    spark2.stop()
    sc1.stop()
    sc2.stop()


/*
    val spark1 = SparkSession.builder()
      .master("local[1]").appName("test")
      .config("hive.metastore.uris", "thrift://eraser-node-2.fyre.ibm.com:9083")
      .config("spark.ui.port", "4042")
      .enableHiveSupport()
      .create()

    println(spark1.toString)
    spark1.sql("show databases").show()
    spark1.sql("show tables").show()

    val spark2 = SparkSession.builder()
      .master("local[1]").appName("test")
      .config("hive.metastore.uris", "thrift://hulks1.fyre.ibm.com:9083")
      .config("spark.ui.port", "4041")
      .enableHiveSupport()
      .create()

    println(spark2.sparkContext.getConf.get("hive.metastore.uris"))

    println(spark2.toString)
    spark2.sql("show databases").show()
    spark2.sql("show tables").show()

    spark1.sql("show databases").show()
    spark1.sql("show tables").show()

    spark1.stop()
    spark2.stop()
    */
  }
}
