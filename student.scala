package com.gyq.spark.core.student
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object student {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val sparConf = new SparkConf().setMaster("local").setAppName("student")
    val sc = new SparkContext(sparConf)

    var rdd = sc.textFile("grade.txt")
    val subjectMap = rdd.map(x => (x.split(",")(0),x.split(",")(2).toInt))
//    println("各科成绩总和:")
//    val sumScoreResult = subjectMap.reduceByKey(_+_).collect().foreach(println)
    println("各科成绩平均值:")
    val avgScoreResult = subjectMap.combineByKey(
      (v) => (v,1),
      (accu:(Int,Int),v) => (accu._1 + v,accu._2 + 1),
      (accu1:(Int,Int),accu2:(Int,Int)) => (accu1._1 + accu2._1, accu1._2 + accu2._2)
    ).mapValues(x => (x._1 / x._2).toDouble).collect().foreach(println)
    println("------------------------------\n各科参加的人数:")
    rdd.map(_.split(",")(0)).groupBy(sub=>sub)
      .map{case (word, list) => {(word, list.size)}}
        .collect().foreach(println)
    println("------------------------------\n各科成绩的最大值:")
    val maxScoreResult = subjectMap.groupByKey().map(x => {
      var max = Integer.MIN_VALUE
      for(num<- x._2){
        if(num > max) {
          max = num
        }
      }
      (x._1,max)
    }).collect().foreach(println)

    sc.stop()
  }
}
