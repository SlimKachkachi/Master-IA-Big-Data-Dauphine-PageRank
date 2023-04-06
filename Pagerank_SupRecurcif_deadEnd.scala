// Databricks notebook source
// DBTITLE 1,Essai de suppression des descends par récursivité
// MAGIC %md

// COMMAND ----------

val links2=sc.parallelize(Array(("E",Array[String]()),("C",Array("E")),("A",Array("E","B","C","D")),("D",Array("B","C")),("B",Array("A","D","E")),("A",Array("B","C","D","B","B"))))
//(,,("C",Array("A")),
//val essai = links2.collect().toList
//essai(1)._2.size

// COMMAND ----------

def dedoublon(links :List[(String, Array[String])],index :Int): List[(String, Array[String])] = {
  if (index == links.size) 
    return links.filter(x => x._2.size != 0)
  else
    if (links(index)._2.size == 0)
        return dedoublon(del_unitaireList(links,links(index)._1),0)
    else
        return dedoublon(links,index+1)
  }

// fonction pour supprimer une valeur de ttes les values / version List
def del_unitaireList(links :List[(String, Array[String])],valeur : String) = {
  links.map{ case(k,v) => (k,v.filter(x => x != valeur))}.filter (_._1 != valeur)}

