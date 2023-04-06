// Databricks notebook source
// DBTITLE 1,Graphe réduit à quatre noeuds (A,B,C,D) et PageRank non optimisé
// MAGIC %md

// COMMAND ----------

// DBTITLE 0,Graphe réduit à quatre noeuds (A,B,C,D) et PageRank non optimisé
// MAGIC %md Partie 1) : Calcul du pagerank avec le graphe (A,B,C,D) selon 3 différents cas (sans spider trap & dead end, avec spider trap, avec deadend)

// COMMAND ----------

// MAGIC %md graphe de l'article fig 5.1 sans 'spider trap' ni 'dead-end'

// COMMAND ----------


//Initialisation du pagerank à (1,1...1)  (et non 1/k)

//essai sans taxation
//res0: Array[(String, Double)] = Array((A,1.3330078125), (B,0.8889973958333333), (C,0.8889973958333333), (D,0.8889973958333333))

//essai avec taxation
//res1: Array[(String, Double)] = Array((A,1.298188273285468), (B,0.9006039089048439), (C,0.9006039089048439), (D,0.9006039089048439))

var ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1))).mapValues(_.toDouble)
val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C"))))

// COMMAND ----------


//Initialisation du pagerank à (1/n,1/n...1/n) 

// résultat avec taxation
//2: Array[(String, Double)] = Array((A,0.324547068321367), (B,0.22515097722621097), (C,0.22515097722621097), (D,0.22515097722621097))

//résultat sans taxation
//res3: Array[(String, Double)] = Array((A,0.333251953125), (B,0.22224934895833331), (C,0.22224934895833331), (D,0.22224934895833331))

val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("A")),("D",Array("B","C"))))

var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// MAGIC %md Graphe avec spider trap

// COMMAND ----------

//graphe de l'article fig 5.6 avec spider trap
// initialisation du pageranks à (1/n, 1/n...1/n)

//taxation
//res6: Array[(String, Double)] = Array((A,0.0819364986973263), (B,0.10505493582860226), (C,0.7079536296454688), (D,0.10505493582860226))

//sans taxation
//res5: Array[(String, Double)] = Array((A,0.008034537358539094), (B,0.011709707754629628), (C,0.9685460471322016), (D,0.011709707754629628))

val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("C")),("D",Array("B","C"))))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// MAGIC %md Graphe avec deux niveaux de deadends

// COMMAND ----------


//initialisation du pagerank à (1/n,1/n..1/n) 

//avec taxation
//Array((A,0.06599450089874627), (B,0.08469294289673751), (C,0.08469294289673751), (D,0.08469294289673751), (E,0.10198900179749254))

//sans taxation converge vers 0
//Array((A,1.1460050840886826E-5), (B,1.670218875177427E-5), (C,1.670218875177427E-5), (D,1.670218875177427E-5), (E,2.2920101681773652E-5))

//avec taxation mais une fois tous les dead end supprimés
//Array[(String, Double)] = Array((A,0.23391812865497083), (B,0.43274853801169566), (D,0.33333333333333326))


val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("D",Array("B","C")),("C",Array("E")),("E",Array[String]//())))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1),("E",1)))

// essai intermédiaire pour vérifier le pagerank aboutit aux mêmes résultats une fois les deadends supprimés                       
//val links=sc.parallelize(Array(("A",Array("B","D")),("B",Array("A","D")),("D",Array("B"))))
//var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("D",1)))

var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// MAGIC %md Graphe avec deux niveaux de deadends / cas d'une structure List() et non Array()

// COMMAND ----------

// essai avec list et non plus array / on obtient les mêmes résultats :-°/
//graphe de l'article fig 5.4 avec 2 niveaux de dead ends 
////code exact du cours avec le vecteur initial coordonnée à 1/K  (et non 1)

// sans taxation 
//Array[(String, Double)] = Array((A,0.08345150254697631), (B,0.10726293468943901), (C,0.10726293468943901), (D,0.10726293468943901))
//avec taxation 
//Array[(String, Double)] = Array((A,0.08345150254697631), (B,0.10726293468943901), (C,0.10726293468943901), (D,0.10726293468943901))

val links=sc.parallelize(Array(("A",List("B","C","D")),("B",List("A","D")),("D",List("B","C")),("C",List())))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// MAGIC %md Corps principal du calcul du pagerank

// COMMAND ----------

// Corps principal de l'algo 
// penser à ré initialiser ranks !! pour les différents essais


println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} ")

var t0= System.nanoTime()
val Iterations = 40
for (i <- 1 to Iterations) {
  val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }
 // ranks=contribs.reduceByKey(_+_).mapValues(1*_)  // essai sans taxation
 // ranks=contribs.reduceByKey(_+_).mapValues(.15+.85*_) //essai avec taxation 
  ranks=contribs.reduceByKey(_+_).mapValues(.15/len_ranks+.85*_) //essai avec v0 initilisée à 1/n pour chaque 'pagerank'
 
 // for (i <- ranks.collect()) println(i)
  println(s"Nbre de partitions de ranks au départ ${ranks.getNumPartitions} " + s" au l'itération ${i}")
}
println(s"Durée de l'exécution ${(System.nanoTime()-t0)/1000000} " )
//for (i <- ranks.collect()) println("%.3f".format(i._2))

val affichage = ranks.collect()
    affichage.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

// COMMAND ----------

// DBTITLE 1,Partie 2) Observation de la distrubtion des deux RDD (links et ranks) sur les différentes partitions
// MAGIC %md Partie 2) Observation de la distrubtion des deux RDD (links et ranks) sur les différentes partitions

// COMMAND ----------

// MAGIC %md 1ier essai sans définition de partitionning

// COMMAND ----------

// exécution pas à pas pour observer la distribution des clés sur les partitions
val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("C")),("D",Array("B","C"))))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

// nombre de partitions par défaut 
//links.getNumPartitions // res2: Int = 8
//ranks.getNumPartitions // res3: Int = 8

// COMMAND ----------

val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }

// COMMAND ----------

ranks=contribs.reduceByKey(_+_).mapValues(.15/len_ranks+.85*_)

// COMMAND ----------

//Récupération de l'indice de la partition 
//links.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
// res2: Array[String] = Array(A#1, B#3, C#5, D#7) 1iere itération

//ranks.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
// res4: Array[String] = Array(A#1, B#3, C#5, D#7) 1iere itération
//res6: Array[String] = Array(A#1, B#2, C#3, D#4). 2ieme itération
//res14: Array[String] = Array(A#1, B#2, C#3, D#4) 3ieme itération 

//contribs.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() // res6: Array[String] = //Array(A#0, D#0, B#0, C#0, B#1, C#1, D#1, A#1) 1iere itération

// COMMAND ----------

// MAGIC %md 2ieme essai en imposant un HasPartitioner(2) sur links

// COMMAND ----------

import org.apache.spark.HashPartitioner

val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("C")),("D",Array("B","C")))).partitionBy(new HashPartitioner(2))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1)))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }

// COMMAND ----------

ranks=contribs.reduceByKey(_+_).mapValues(.15/len_ranks+.85*_)

// COMMAND ----------

//Récupération de l'indice de la partition 
//links.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
// res15: Array[String] = Array(B#0, D#0, A#1, C#1) 1iere itération

ranks.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
//res16: Array[String] = Array(A#1, B#3, C#5, D#7)) 1iere itération
//res18: Array[String] = Array(B#0, D#0, A#1, C#1). 2ieme itération
//res19: Array[String] = Array(B#0, D#0, A#1, C#1) 3ieme itération 

//contribs.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() // res6: Array[String] = res17: Array[String] = Array(A#0, D#0, B#0, C#0, B#1, C#1, D#1, C#1) 1iere itération

// COMMAND ----------

// MAGIC %md 3ieme essai : en imposant un Haspartioner sur les deux RDD

// COMMAND ----------

import org.apache.spark.HashPartitioner

val links=sc.parallelize(Array(("A",Array("B","C","D")),("B",Array("A","D")),("C",Array("C")),("D",Array("B","C")))).partitionBy(new HashPartitioner(2))
var prepa_ranks=sc.parallelize(Array(("A",1),("B",1),("C",1),("D",1))).partitionBy(new HashPartitioner(2))
var len_ranks=prepa_ranks.count()
var ranks = prepa_ranks.mapValues(_.toDouble/len_ranks)

// COMMAND ----------

val contribs = links.join(ranks).flatMap {
    case (aero,(vol,rank)) => vol.map(dest => (dest, rank/vol.size))
  }

// COMMAND ----------

ranks=contribs.reduceByKey(_+_).mapValues(.15/len_ranks+.85*_)

// COMMAND ----------

//Récupération de l'indice de la partition 
//links.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
// res21: Array[String] = Array(B#0, D#0, A#1, C#1) 1iere itération

ranks.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() 
//res22: Array[String] = Array(B#0, D#0, A#1, C#1) 1iere itération
//res24: Array[String] = Array(B#0, D#0, A#1, C#1). 2ieme itération
//res24: Array[String] = Array(B#0, D#0, A#1, C#1) 3ieme itération 

//contribs.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => x._1 + "#" + index).iterator }.collect() // res6: Array[String] = res17: Array[String] = Array(A#0, D#0, B#0, C#0, B#1, C#1, D#1, C#1) 1iere itération
