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
    val community: Set[Long] = Set(236l, 186l, 88l, 213l)

    graph.mapTriplets(e => e.attr + triplet_modularity(community, e))

    graph.edges.foreach(e => {
      if (e.attr != 0.0) println
    })
    //    Recollect all commscore dispersed into the graph
    //    todo do i need to fold them all?

    //    println(graph.edges.reduce((a, b) => {Edge(-1, -1, a.attr + b.attr)}).attr)
    //    normRDD.aggregate(0)((acc:Double, edge:Edge[Double]) => Double {acc + edge.attr})((val1:Double, val2:Double) => Double {val1 + val2})

    //    val result = graph.edges.aggregate(0.0)(
    //      (m, e) => {e.attr + m},
    //      (m1, m2) => {m1 + m2})
    //    )

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
    println(s"edgeTriplet: ${edge.toString}")

    if ((set.contains(edge.srcId) && !set.contains(edge.dstId)) || (!set.contains(edge.srcId) && set.contains(edge.dstId))) {
      println(s" 1 * (${edge.srcAttr} * ${edge.dstAttr}) / (2 * $edge_number)")
      val tmp_debug: Double = 1.0 * (edge.srcAttr * edge.dstAttr) / (2.0 * edge_number)
      println(tmp_debug)
      tmp_debug
      //          1.0 * (edge.srcAttr * edge.dstAttr) / (2.0 * edge_number)
    } else {
      10.0
    }

    //    Fake return
    //    0.0
  }
}
