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
    //    println(s"edge: ${finalGraph.edges.first()}, vertex: ${finalGraph.vertices.first()}")
    //    println(s"FG: edge: ${finalGraph.edges.count()}")
    compute_modularity(finalGraph)

    //    println("Press any key to exit")
    //    StdIn.readLine()
  }

  def compute_modularity(graph: Graph[Int, Double]): Double = {
    println("////////////////////")
    val community: Set[Long] = Set(236l, 186l, 88l, 213l, 315l, 3465l, 456l, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val comGraph = graph.mapTriplets(e => triplet_modularity(community, e)).edges.filter(e => e.attr != 0.0)


    val comScore: Double = 1 / ((4 * edge_number) * comGraph.aggregate(0.0)((acc: Double, e: Edge[Double]) => acc + e.attr, (d1: Double, d2: Double) => d1 + d2))

    //    (1 / (4 * edge_numb) * com_modularity)

    println(s"ComScore: $comScore, countInGraph: ${comGraph.count()}")
    println("////////////////////")
    0.3
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
