package com.gerritforge.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Inside, Matchers}


class SparkJoinSpec extends FlatSpec with Matchers with Inside {

  "spark" should "load JSON as RDD" in {

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()

    def getContentLines(s: String) = {
      val html = scala.io.Source.fromURL(s"https://raw.githubusercontent.com/pakkio/tmp/master/$s").mkString
      html.split("\n").filter(_ != "")
    }

    def getFileContentAsRDD(s: String)(implicit spark:SparkSession): RDD[String] = {
      val list = getContentLines(s)
      spark.sparkContext.parallelize(list)
    }
    // Get json from my github pakkio/tmp project
    def getGitJson(s: String)(implicit spark:SparkSession) = {
      spark.read.json(getFileContentAsRDD(s))
    }


    val orgs = getGitJson("organizations.json")
    val analyticsData = getGitJson("analytics.json")
    val joined = analyticsData.join(orgs,Seq("email"),"left_outer")

    orgs.show(10)

    analyticsData.show()

    joined.na.fill("indipendent").show(100)

  }


}
