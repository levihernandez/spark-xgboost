package com.cloudera.spark.ml

/**
 * Modified/Run by Julian Levi Hernandez 3/28/2020
 * Test XGBoost Code in CDH 6.0.0 Hadoop 3
 * - Configured pom.xml to properly import CDH 6.0.0 Spark libraries
 * - Configured pom.xml dependencies, plugins to import XGBoost Library
 * $SPARK_HOME/bin/spark-submit --verbose --class com.cloudera.spark.ml.App" --master local[*] --jars /home/user/xgboost_2.11-0.1.0-SNAPSHOT.jar "filepath: /home/user/iris.csv"
 * code sample used from tutorials:
 * https://github.com/CodingCat/xgboost4j-spark-scalability/blob/master/src/main/scala/me/codingcat/xgboost4j/AirlineClassifier.scala
 * https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html#
 * https://docs.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_running_spark_on_yarn.html
 */

import com.sun.research.ws.wadl.Application
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier


object App extends Application {

  def main(args: Array[String]) {

    println(s"[jlevi-debug] Start Spark ML job - XGBoost")
    // Create SparkSession
    val spark = SparkSession
      .builder().appName("XGBoost Demo Spark ML Application")
      .getOrCreate()


    try {
      // get path to file in HDFS
      val fin = args(0) // file path parsed location

      // sepal_length,sepal_width,petal_length,petal_width,species
      val schema = new StructType(Array(
        StructField("sepal_length", DoubleType, true),
        StructField("sepal_width", DoubleType, true),
        StructField("petal_length", DoubleType, true),
        StructField("petal_width", DoubleType, true),
        StructField("class", StringType, true))
      )

      println(s"[jlevi-debug] Read file and assign to schema")
      val rawInput = spark.read.schema(schema).csv(fin)
      val stringIndexer = new StringIndexer()
        .setInputCol("class")
        .setOutputCol("classIndex")
        .fit(rawInput)

      println(s"[jlevi-debug] Transform Raw Input")
      val labelTransformed = stringIndexer
        .transform(rawInput)
        .drop("class")

      println(s"[jlevi-debug] Vectorize Data")
      val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
        .setOutputCol("features")

      println(s"[jlevi-debug] Transform Data")
      val xgbInput = vectorAssembler
        .transform(labelTransformed)
        .select("features", "classIndex")

      println(s"[jlevi-debug] Preview vectorized records")
      xgbInput.show() // Preview records transformed into vectors

      val xgbParam = Map("eta" -> 0.1f,
        "missing" -> -999,
        "objective" -> "multi:softprob",
        "num_class" -> 3,
        "num_round" -> 100,
        "num_workers" -> 24)

      val xgbClassifier = new XGBoostClassifier(xgbParam)
        .setFeaturesCol("features")
        .setLabelCol("classIndex")

      //xgbClassifier.setMaxDepth(2)
      val xgbClassificationModel = xgbClassifier
        .fit(xgbInput)
      println(s"[jlevi-debug] Create Model")
      val predicted = xgbClassificationModel
        .transform(xgbInput)
      println(s"[jlevi-debug] Predict Outcomes")
      predicted.show() // preview predictions
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(0)
    }
  }


}

