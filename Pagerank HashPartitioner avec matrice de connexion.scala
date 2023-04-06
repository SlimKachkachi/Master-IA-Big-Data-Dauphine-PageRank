// Databricks notebook source
// DBTITLE 1,Pagerank_variante_matrice_connexion
// MAGIC %md

// COMMAND ----------

import org.apache.spark.HashPartitioner

//val path = "/FileStore/tables/Graphe/"  // pour exécution sur DataBrick
val path= "/students/iasd_20222023/skachkachi/datasets/" // pour exécution sur cluster Lamsade
val fileRDD = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys

val FligthRDD = fileRDD.map{ row => row.split(',').map(_.trim)}.map(x => (x(3).toInt,Array.fill[Int](1)(x(4).toInt)))
val prepa_ranks=FligthRDD.groupByKey().map{case(x,y) => (x,1)}
val links=FligthRDD.reduceByKey(_++_).map{case (a,b) => (a,(b,b.size))}.partitionBy(new HashPartitioner(8))
links.cache

var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

val Iterations = 50
var doNotStop = 1
val epsilon = 0.001
var i = 0

var t0= System.nanoTime()

while(i < Iterations && doNotStop  > 0) {
  val contribs = links.join(ranks).flatMap {
    case (aero,((vol,size),rank)) => vol.map(x => (x, rank/size))
  }
  var new_ranks=contribs.reduceByKey(_+_).mapValues(0.15/len_ranks+0.85*_)
  
  doNotStop=new_ranks.join(ranks).map{case (aero,(newR,oldR)) => if (oldR > newR) (oldR - newR) else (newR - oldR) }.filter(x => x > epsilon).count().toInt
  i=i+1
  ranks=new_ranks
  println(s"Niveau de précision suffisant${doNotStop} " )
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
val affichage = ranks.collect()
    affichage.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))


// COMMAND ----------

// nouvelle formule
  val contribs = links1.join(ranks).flatMap {
    case (aero,((vol,size),rank)) => vol.map(x => (x, rank/size))}
