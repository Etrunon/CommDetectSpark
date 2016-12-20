package MyGraph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// ToDo's
// Mettere negli archi i gradi dei vertici
// Mettere nei vertici il loro apporto di comunità
// Pregelare

/**
  * Created by etrunon on 01/12/16.
  */
object entry {
  //  Total edge number in the graph
  var edge_number: Long = -1

  def main(args: Array[String]): Unit = {

    //    Spark Configuration
    val conf = new SparkConf().setAppName("MyGraph").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val mygutil = new MyGraphUtility(sc)
    val file = "Data/facebook-cleaned/total.csv"

    val loadedGraph = Graph.fromEdgeTuples(mygutil.getEdgeTuples(file), 0)
    //    Save the total edge number for later use
    edge_number = loadedGraph.edges.count()

    //    Create a cleaned version with some help data
    val edgeRDD: RDD[Edge[Int]] = mygutil.readEdges(file)
    val vertexRDD: RDD[(VertexId, (Int, Double, Int))] = loadedGraph.degrees.join(loadedGraph.vertices).map({ case (id: Long, (deg: Int, att: Int)) => (id, (deg, 0.0d, -1)) })

    val finalGraph = Graph[(Int, Double, Int), Int](vertexRDD, edgeRDD, (-1, -1.0, -1))

    finalGraph.triplets.foreach(println)

    finalGraph.pregel(Double.NegativeInfinity)((id: VertexId, data: (Int, Double, Int), newDist) => println(s"Id: $id, Data: $data"))

    //    expand_modularity_from(finalGraph, Set(0)

    //    println("Press any key to exit")
    //    StdIn.readLine()
  }

  /**
    * Compute the modularity on the starting set and try to expand, hopefully, it into the best with greedy-like approach.
    * Still working on it
    *
    * @param graph     on which operate
    * @param nodeStart set of inizial community
    * @return
    */
  def expand_modularity_from(graph: Graph[Int, Double], nodeStart: Set[Long]): (Double, Set[Long]) = {
    //    Output separator
    println("//Open///" + "//////" * 6)
    val baseMod = compute_modularity(graph, nodeStart)
    val neighbours = graph.triplets.collect({ case t if nodeStart.contains(t.srcId) && !nodeStart.contains(t.dstId) => t.dstId }).foreach(println)

    //    println(s"BaseScore: $baseMod, baseSet: $nodeStart.\t\t finalScore: $resScore, finalSet: $resSet")
    println("/////" * 6)
    // Fake return
    (-1.0, Set(-1l))
  }

  /**
    * Compute the modularity score of the given to-be tested nodes on the provided graph.
    *
    * @param graph     graph on which test the community
    * @param community set of nodes inside the comm
    * @return
    */
  def compute_modularity(graph: Graph[Int, Double], community: Set[Long]): Double = {
    //    Output separator
    println("##Open###" + "######" * 6)
    //
    val comGraph = graph.mapTriplets(e => triplet_modularity(community, e)).edges.filter(e => e.attr != 0.0)
    val comScore: Double = 1 / ((4 * edge_number) * comGraph.aggregate(0.0)((acc: Double, e: Edge[Double]) => acc + e.attr, (d1: Double, d2: Double) => d1 + d2))

    println(s"ComScore: $comScore, countInGraph: ${comGraph.count()}")
    println("#####" * 6)
    comScore
  }

  /**
    * This function compute the modularity contribution of each node and put it into the edge attribute
    *
    * @param set  of to-be tested community
    * @param edge triplet
    * @return
    */
  def triplet_modularity(set: Set[Long], edge: EdgeTriplet[Int, Double]): Double = {
    //    println(s"edgeTriplet: ${edge.toString}")

    if ((set.contains(edge.srcId) && !set.contains(edge.dstId)) || (!set.contains(edge.srcId) && set.contains(edge.dstId))) {
      val tmp_debug: Double = 1.0 * (edge.srcAttr * edge.dstAttr) / (2.0 * edge_number)
      //      println(s" 1 * (${edge.srcAttr} * ${edge.dstAttr}) / (2 * $edge_number) ==> $tmp_debug")
      tmp_debug
      //          1.0 * (edge.srcAttr * edge.dstAttr) / (2.0 * edge_number)
    } else {
      0.0
    }
  }
}
