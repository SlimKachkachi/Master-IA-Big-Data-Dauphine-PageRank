// Databricks notebook source
// DBTITLE 1,Pagerank avec le HashPartitioner
// MAGIC %md

// COMMAND ----------

// MAGIC %md codes pour les fichiers CVS

// COMMAND ----------

import org.apache.spark.HashPartitioner

//val path = "/FileStore/tables/Graphe/"  // pour exécution sur DataBrick
val path= "/students/iasd_20222023/skachkachi/datasets/" // pour exécution sur cluster Lamsade
val fileRDD = sc.textFile(path + "201412.csv").zipWithIndex().filter(_._2 > 0).keys


val FligthRDD = fileRDD.map{ row => row.split(',').map(_.trim)}.map(x => (x(3).toInt,Array.fill[Int](1)(x(4).toInt)))
val prepa_ranks=FligthRDD.groupByKey().map{case(x,y) => (x,1)}
val links=FligthRDD.reduceByKey(_++_).map{case (a,b) => (a,b)}.partitionBy(new HashPartitioner(24))
links.cache

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
  println(s"Niveau de précision suffisant${doNotStop} " )
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )

val affichage = ranks.collect()
    affichage.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))


// COMMAND ----------

// DBTITLE 1,Essais sur le mini graphe
// MAGIC %md

// COMMAND ----------

//code exact du cours avec le vecteur initial coordonnée à 1 (et non 1/k)  avec hashpartitioner()
//pour rappel on arrive à une situation de copartitioner et colocalisation

//graphe identique.à l'article figure 5.1 => porportion respectée avec le vO en 1/k 
// avec v0 = 1 et la taxation
//res25: Array[(String, Double)] = Array((A,1.298188273285468), (B,0.9006039089048439), (C,0.9006039089048439), (D,0.9006039089048439))

// avec v0 = 1 sans la taxation
//res26: Array[(String, Double)] = Array((A,1.3330078125), (B,0.8889973958333333), (C,0.8889973958333333), (D,0.8889973958333333))


var ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1))).mapValues(_.toDouble)
val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C")))).partitionBy(new HashPartitioner(8))
links.cache

println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")

var t0= System.nanoTime()
val Iterations = 10
for (i <- 1 to Iterations) {
  val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }
//   ranks=contribs.reduceByKey(_+_).mapValues(1*_)
   ranks=contribs.reduceByKey(_+_).mapValues(.15+.85*_)
//  for (i <- ranks.collect()) println(i)
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
ranks.collect()

// COMMAND ----------

//essai avec une boucle de contrôle sur la convergence des pagerank
// graphe identique à l'article fig 5.1
//hashpartitioner
//v0 en 1/K + taxation
//ici on fait une jointure sur des tables qui ont l'unicité sur les clés cas idéal
//res5: Array[(String, Double)] = Array((A,0.3247481431777954), (B,0.2250839522740682), (C,0.2250839522740682), (D,0.2250839522740682))
import org.apache.spark.HashPartitioner
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)
val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C")))).partitionBy(new HashPartitioner(8))
links.cache
//val beta = 0.85 attention passer beta en paramètre semble ralentir de façon importante l'execution

println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")

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
  println(s"Niveau de précision suffisant${doNotStop} " )
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
ranks.collect()


// COMMAND ----------

//code exact du cours avec le vecteur initial coordonnée à 1/k  (et non 1)  et avec hashpartitioner()
// graphe identique à l'article avec V0 1/K  sans la taxation et on obtient bien le même résultat
//res22: Array[(String, Double)] = Array((A,0.333251953125), (B,0.22224934895833331), (C,0.22224934895833331), (D,0.22224934895833331))

// essai avec 0,85*M +0,15/k on obtient un résultat quasi identique
//res23: Array[(String, Double)] = Array((A,0.324547068321367), (B,0.22515097722621097), (C,0.22515097722621097), (D,0.22515097722621097))

// essai avec 0,85*M +0,15 on obtient un résultat différent et non  proportionnellement correct
//res24: Array[(String, Double)] = Array((A,1.1013619339848675), (B,0.769338284330988), (C,0.769338284330988), (D,0.769338284330988))

val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C")))).partitionBy(new HashPartitioner(8))
links.cache

var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

//val beta = 0.85 attention passer beta en paramètre semble ralentir de façon importante l'execution

println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")
var t0= System.nanoTime()
val Iterations = 10

for (i <- 1 to Iterations) {
  val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }
 ranks=contribs.reduceByKey(_+_).mapValues((0.15/len_ranks)+0.85*_) //res23
//  ranks=contribs.reduceByKey(_+_).mapValues(1*_)                  //res22
//  ranks=contribs.reduceByKey(_+_).mapValues(0.15+ 0.85*_)                  //res24
  
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
ranks.collect()

// COMMAND ----------

//contrôle des index des partitions utilisées

//ranks.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect()
//res0: Array[String] = Array(15412#0, 12266#0, 13296#0, 11042#0, 14492#0, 13930#0, 11540#0, 11618#0, 15624#0, 15370#0, 11996#0, 10397#1, //10599#1, 13495#1, 11433#1, 10423#1, 12217#1, 15919#1, 12951#1, 11057#1, 10821#1, 14107#1)

links.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect()
//res1: Array[String] = Array(15412#0, 13296#0, 14492#0, 11540#0, 15624#0, 11996#0, 10397#1, 11433#1, 12217#1, 11057#1, 10821#1, 12266#2, 11042#2, 13930#2, 11618#2, 15370#2, 10599#3, 13495#3, 10423#3, 15919#3, 12951#3, 14107#3)
