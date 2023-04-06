// Databricks notebook source
// DBTITLE 1,Pagerank non optimisé
// MAGIC %md

// COMMAND ----------

// MAGIC %md Code pour les fichiers CSV

// COMMAND ----------

// version avec array pour links

//val path = "/FileStore/tables/Graphe/"  //sur DataBrick
val path= "/students/iasd_20222023/skachkachi/datasets/" //sur Cluster Lamsade
val fileRDD = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys

val FligthRDD = fileRDD.map{ row => row.split(',').map(_.trim)}.map(x => (x(3).toInt,Array.fill[Int](1)(x(4).toInt)))
val prepa_ranks=FligthRDD.groupByKey().map{case(x,y) => (x,1)}
val links=FligthRDD.reduceByKey(_++_).map{case (a,b) => (a,b)}
links.cache

var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)


// COMMAND ----------

// pour vérifier qu'il n'y a pas de deadend
links.filter(x => x._2.size == 0).count()

// COMMAND ----------

//contrôle des parititons

//links.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect()
//res1: Array[String] = Array(14696#0, 12264#0, 14828#0, 12278#0, 15024#0, 11292#0, 15370#0, 10868#0, 10620#0, 11146#0, 10918#0...

//ranks.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect()
//res2: Array[String] = Array(14696#0, 12264#0, 10930#0, 12278#0, 10268#0, 11292#0, 13184#0, 10868#0, 10620#0, 12896#0, 10918#0...

// COMMAND ----------

// version avec list pour links

//val path = "/FileStore/tables/Graphe/" 
val path= "/students/iasd_20222023/skachkachi/datasets/" //sur Cluster Lamsade
val fileRDD = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys
//head=fileRDD.first() ou head=fileRDD.take(1) autre solution
//val fileRDD = sc.textFile(path + "201201_Reduit.csv").filter(_ != head) autre solution

val FligthRDD = fileRDD.map{ row => row.split(',').map(_.trim)}.map(x => (x(3),x(4)))
val links=FligthRDD.groupByKey().map{case(k,v) => (k,v.toList)}
//links.cache

val prepa_ranks=FligthRDD.groupByKey().map{case(x,y) => (x,1)}
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// code avec le contrôle de la convergence des pageranks

  val Iterations = 50
  var doNotStop = 1
  val epsilon = 0.001
  var i = 0

  var t0= System.nanoTime()

  while(i < Iterations && doNotStop  > 0) {
    val contribs = links.join(ranks).flatMap {
      case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
    }
    var new_ranks=contribs.reduceByKey(_+_).mapValues(0.15/len_ranks+0.85*_)

    doNotStop=new_ranks.join(ranks).map{case (aero,(newR,oldR)) => if (oldR > newR) (oldR - newR) else (newR - oldR) }.filter(x => x > epsilon).count().toInt
    i=i+1
    ranks=new_ranks
    println(s"Niveau de précision insuffisant ${doNotStop} " )
    println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
  }
  println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )

  val affichage = ranks.collect()
      affichage.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))



// COMMAND ----------

//suite à supprimer à la fin

// COMMAND ----------

//essai basis-c avec algo non optimisé et vO en 1/K

var t0= System.nanoTime()
val Iterations = 10

for (i <- 1 to Iterations) {
  val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }
  ranks=contribs.reduceByKey(_+_).mapValues((0.15/len_ranks)+0.85*_) 
  
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
ranks.collect()
