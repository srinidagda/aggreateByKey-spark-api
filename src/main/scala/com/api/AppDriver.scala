package com.api

import org.apache.spark.{SparkConf, SparkContext}

object AppDriver {
  def main(args:Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey-spark-api-v2")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    studentwiseMaxMarks(sc)
    studentMaxMarks(sc)
    studentSubjectWiseMaxMarks2(sc)
    studentAvgMarks(sc)
    studentAvgMarks2(sc)
    sc.stop()
  }

  def studentwiseMaxMarks(sc:SparkContext): Unit = {
    val marksData = sc.textFile("/Users/srini/marks.txt")
    val zero = ("", 0)
    val studentWiseMaxMarks = marksData.map(data => data.split(","))
      .map(studentData => (studentData(0).trim, (studentData(1).trim, studentData(2).trim.toInt)))
      .aggregateByKey(zero)(seqOpSubjectWise, combineOp)
    studentWiseMaxMarks.collect().foreach(println(_))
  }

  def seqOpSubjectWise = (accumulator:(String, Int), element:(String, Int)) =>
    if (accumulator._2 > element._2) accumulator else element

  def combineOp = (accumulator1:(String, Int), accumulator2:(String, Int)) => if (accumulator1._2 > accumulator2._2) accumulator1 else accumulator2

  def studentMaxMarks(sc:SparkContext): Unit = {
    val marksData = sc.textFile("/Users/srini/marks.txt")
    val zero = 0
    val maxMarks = marksData.map(data => data.split(","))
      .map(marks => (marks(0).trim,(marks(1).trim, marks(2).trim.toInt)))
      .aggregateByKey(zero)(seqOpMarks,combineMarks)
    maxMarks.collect().foreach(println(_))
  }
  def seqOpMarks = (accumulator:Int, element:(String, Int)) => if (accumulator > element._2) accumulator else element._2
  def combineMarks = (accumulator1:Int, accumulator2:Int) => if (accumulator1 > accumulator2) accumulator1 else accumulator2

  def studentSubjectWiseMaxMarks2(sc:SparkContext): Unit = {
    val marksData = sc.textFile("/Users/srini/marks.txt")
    val marks = marksData.map(data => data.split(",")).map(marks => (marks(0).trim, (marks(1).trim, marks(2).trim.toInt)))
      .combineByKey(createCom, mergeVal, mergeCombin)
    marks.collect().foreach(println(_))
  }
  def createCom = (accumulator:(String, Int)) => (accumulator._1, accumulator._2)
  def mergeVal = (accumulator:(String, Int), element:(String, Int)) => if (accumulator._2 > element._2) accumulator else element
  def mergeCombin = (accumulator1:(String, Int), accumulator2:(String, Int)) => if (accumulator1._2 > accumulator2._2) accumulator1 else accumulator2

  def studentAvgMarks(sc:SparkContext): Unit = {
    val marksData = sc.textFile("/Users/srini/marks.txt")
    val zero = (0.0, 0.0)
    val maxMarks = marksData.map(data => data.split(",")).map(marks =>  (marks(0).trim, (marks(1).trim, marks(2).trim.toDouble)))
      .aggregateByKey(zero)(seqOpAvg, combineAvg).map(e => (e._1, (e._2._1/e._2._2)))
    maxMarks.collect().foreach(println(_))
  }
  def seqOpAvg = (accumulator:(Double, Double), element:(String, Double)) => (accumulator._1+element._2, accumulator._2+1)

  def combineAvg = (accumulator1:(Double, Double), accumulator2:(Double, Double)) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)

  def studentAvgMarks2(sc:SparkContext): Unit = {
    val marksData = sc.textFile("/Users/srini/marks.txt")
    val maxMarks = marksData.map(data=>data.split(",")).map(marks => (marks(0).trim, (marks(1).trim, marks(2).trim.toInt)))
      .combineByKey(createComAvg, mergeValAvg, mergeCombinAvg)
      .map(e => (e._1, (e._2._1/e._2._2)))
    maxMarks.collect().foreach(println(_))
  }

  def createComAvg = (data:(String, Int)) => (data._2.toDouble, 1)

  def mergeValAvg = (accumulator:(Double, Int), element:(String, Int)) => (accumulator._1+element._2, accumulator._2+1)

  def mergeCombinAvg = (accumulator1:(Double, Int), accumulator2:(Double, Int)) => (accumulator1._1+accumulator2._1, accumulator1._2+accumulator2._2)
}
