package MyGraph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by etrunon on 01/12/16.
  */
object entry {
  var edge_number: Long = -1

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyGraph").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mygraph = new MyGraph(sc)
    val file = "Data/facebook-cleaned/total.csv"
    val graph = Graph.fromEdgeTuples(mygraph.getEdgeTuples(file), 0)
    edge_number = graph.edges.count()

    val edgeRDD: RDD[Edge[Double]] = mygraph.readEdges(file)
    //    println(s"edgeRDD: $edgeRDD\t${edgeRDD.first()}")
    val vertexRDD: RDD[(VertexId, Int)] = graph.degrees.join(graph.vertices).map({ case (id: Long, (deg: Int, att: Int)) => (id, deg) })
    //    println(s"vertexRDD: $vertexRDD\t${vertexRDD.first()}")

    val finalGraph = Graph[Int, Double](vertexRDD, edgeRDD, 0)

    expand_modularity_from(finalGraph, Set(0))
    //    println(s"edge: ${finalGraph.edges.first()}, vertex: ${finalGraph.vertices.first()}")
    //    println(s"FG: edge: ${finalGraph.edges.count()}")

    //    println("Press any key to exit")
    //    StdIn.readLine()
  }

  /**
    * Compute the modularity on the starting set and try to expand it into the best with greedy approach
    *
    * @param graph     on which operate
    * @param nodeStart set of inizial community
    * @return
    */
  def expand_modularity_from(graph: Graph[Int, Double], nodeStart: Set[Long]): (Double, Set[Long]) = {
    println("//Open///" + "//////" * 6)
    val baseMod = compute_modularity(graph, nodeStart)
    val neighbours = graph.triplets.collect({ case t if nodeStart.contains(t.srcId) && !nodeStart.contains(t.dstId) => t.dstId }).foreach(println)

    //    var resScore = baseMod
    //    var resSet = nodeStart

    //    val xdsd = graph.triplets.filter(e => nodeStart.contains(e.srcId)).map(e => compute_modularity(graph, nodeStart+e.dstId)).collect().foreach(println)

    //    println(s"BaseScore: $baseMod, baseSet: $nodeStart.\t\t finalScore: $resScore, finalSet: $resSet")
    println("/////" * 6)
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
    println("##Open###" + "######" * 6)
    val comGraph = graph.mapTriplets(e => triplet_modularity(community, e)).edges.filter(e => e.attr != 0.0)
    val comScore: Double = 1 / ((4 * edge_number) * comGraph.aggregate(0.0)((acc: Double, e: Edge[Double]) => acc + e.attr, (d1: Double, d2: Double) => d1 + d2))

    println(s"ComScore: $comScore, countInGraph: ${comGraph.count()}")
    println("#####" * 6)
    comScore
  }

  /**
    * This function compute the modularity contribution of each node and put it into the edge attribute
    *
    * @param set  of tested community
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
