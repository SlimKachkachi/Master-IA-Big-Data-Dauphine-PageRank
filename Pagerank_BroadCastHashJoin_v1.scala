// Databricks notebook source
// DBTITLE 1,Pagerank  avec le BroadCastHashJoin


// COMMAND ----------

// MAGIC %md Code pour les fichiers CSV

// COMMAND ----------


//val path = "/FileStore/tables/Graphe/"  // pour exécution sur Databricks
val path= "/students/iasd_20222023/skachkachi/datasets/" // pour exécution sur cluster Lamsade
val fileRDD = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys

val FligthRDD = fileRDD.map{ row => row.split(',').map(_.trim)}.map(x => (x(3).toInt,Array.fill[Int](1)(x(4).toInt)))
val prepa_ranks=FligthRDD.groupByKey().map{case(x,y) => (x,1)}
val links=FligthRDD.reduceByKey(_++_).map{case (a,b) => (a,b)}
//links.cache

var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)


// COMMAND ----------

// MAGIC %md code pour le BroadCastHashJoin / code tiré du cours et adapté

// COMMAND ----------

import org.apache.spark.rdd.RDD // pas nécessaire sur Databrikcs mais nécessaire sur spark-shell

def BroadCastHashJoin(links :RDD[(Int, Array[Int])], ranks :RDD[(Int, Double)]) = {
  val ranklocal = ranks.collectAsMap()
  val ranklocalBCast = links.sparkContext.broadcast(ranklocal)
  links.mapPartitions( iter => {
    iter.flatMap{
      case (k,v1) => ranklocalBCast.value.get(k) match {
        case None => Seq.empty
        case Some(v2) => Seq((k,(v1,v2)))
      }
    }
  }, preservesPartitioning = true )
}

// COMMAND ----------

// code avec le contrôle de la convergence des pageranks

println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")

val Iterations = 50
var doNotStop = 1
val epsilon = 0.001
var i = 0

var t0= System.nanoTime()

while(i < Iterations && doNotStop  > 0) {
  val contribs = BroadCastHashJoin(links,ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
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

//différents contrôles
//val contribs = BroadCastHashJoin(links,ranks)
//contribs.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 

//var contribs = BroadCastHashJoin(links,ranks).flatMap{
//   case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))}
//contribs.collect()
//contribs.reduceByKey(_+_).mapValues(.15+.85*_).collect()
contribs.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 

// COMMAND ----------

// MAGIC %md Code pour le petit graphe (A,B,C,D)

// COMMAND ----------

var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)
val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C"))))

// COMMAND ----------

// essai avec un broadcast small rdd et partition big rdd avec un map qui est une énorme optimisation par rapport à un join
// import org.apache.spark.rdd.RDD pas nécessaire sur Databrikcs mais nécessiare sur spark-shell
//rappel Links (String, Array[String])
//rappel ranks (String, Double)
//pas de  typage forcé de la fonction :RDD[(String, (Array[String], Double))]

def BroadCastHashJoin(links :RDD[(String, Array[String])], ranks :RDD[(String, Double)]) = {
  val ranklocal = ranks.collectAsMap()
  val ranklocalBCast = links.sparkContext.broadcast(ranklocal)
  links.mapPartitions( iter => {
    iter.flatMap{
      case (k,v1) => ranklocalBCast.value.get(k) match {
        case None => Seq.empty
        case Some(v2) => Seq((k,(v1,v2)))
      }
    }
  }, preservesPartitioning = true )
}

// COMMAND ----------

// code avec le contrôle de la convergence des pageranks

println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")

val Iterations = 50
var doNotStop = 1
val epsilon = 0.001
var i = 0

var t0= System.nanoTime()

while(i < Iterations && doNotStop  > 0) {
  val contribs = BroadCastHashJoin(links,ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
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

// MAGIC %md Annexe : aggrégation des vols pour l'année 2014

// COMMAND ----------

// essai de concatenation de fichiers CSV

//val path = "/FileStore/tables/Graphe/" 
val path= "/students/iasd_20222023/skachkachi/datasets/"  //chemin le cluster Lamsade
val fileRDD12 = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD11 = sc.textFile(path + "201411.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD10 = sc.textFile(path + "201410.csv").zipWithIndex().filter(_._2 > 0).keys //33,9 Mo
val fileRDD09 = sc.textFile(path + "201409.csv").zipWithIndex().filter(_._2 > 0).keys //32,3 Mo
val fileRDD08 = sc.textFile(path + "201408.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD07 = sc.textFile(path + "201407.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD06 = sc.textFile(path + "201406.csv").zipWithIndex().filter(_._2 > 0).keys //33,9 Mo
val fileRDD05 = sc.textFile(path + "201405.csv").zipWithIndex().filter(_._2 > 0).keys //32,3 Mo
val fileRDD04 = sc.textFile(path + "201404.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD03 = sc.textFile(path + "201403.csv").zipWithIndex().filter(_._2 > 0).keys //33,2 Mo
val fileRDD02 = sc.textFile(path + "201402.csv").zipWithIndex().filter(_._2 > 0).keys //33,9 Mo
val fileRDD01 = sc.textFile(path + "201401.csv").zipWithIndex().filter(_._2 > 0).keys //32,3 Mo
val fileRDD= fileRDD12.union(fileRDD11).union(fileRDD10).union(fileRDD09).union(fileRDD08).union(fileRDD07).union(fileRDD06).union(fileRDD05).union(fileRDD04).union(fileRDD03).union(fileRDD02).union(fileRDD01)
}

