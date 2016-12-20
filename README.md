# CommDetectSpark

Repository of thesis work on an implementation of Community Detection algorithms using Spark.

## Use
Run the * entry.scala * script. It loads automatically the *total.tsv* file in the **Data/facebook-cleaned/** folder. 
It contains a graph dataset separated by tabs and tries to detect community, at the moment arbitrarily startin from node 0 and in the 
future trying to identify the best community starting from a node and expanding.
