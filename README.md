# Master-IA-Big-Data-Dauphine-PageRank
Implémentation du PageRank en Scala avec gestion du partitionnement 

Pagerank  (avec taxation) en scala avec gestion sur partitionnement (la matrice de connexion et le pagerank sous forme de RDD)
Les partitioners sont : le hashpartitioner, le broadcastjoinpartitioner

Le calcul du pagerank passe par une jointure et un reducebykey
