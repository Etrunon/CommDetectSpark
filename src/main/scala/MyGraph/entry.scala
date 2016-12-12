package MyGraph

import org.apache.spark.graphx.{EdgeTriplet, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

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

    val edgeRDD = graph.edges
    //    println(s"edgeRDD: $edgeRDD\t${edgeRDD.first()}")
    val vertexRDD = graph.degrees.join(graph.vertices).map({ case (id: Long, (deg: Int, att: Int)) => (id, deg) })

    println(s"vertexRDD: $vertexRDD\t${vertexRDD.first()}")
    val defaultNode = (1, 0)

    val finalGraph = Graph(vertexRDD, edgeRDD, defaultNode)

    //    println(s"FG: edge: ${finalGraph.edges.count()}")
    //    compute_modularity(finalGraph)

    //    graph.mapVertices((v, a) => graph.degrees.lookup(v)).vertices.foreach(println)

    //    graph.edges.foreach(println)

    //    edge_number = graph.edges.count()

    //    compute_modularity(graph)
    //    val value: ArrayB
    //    println(graph.degrees.lookup(213).head)
    //    graph.degrees.foreach(println)

    println("Press any key to exit")
    StdIn.readLine()
  }

  def compute_modularity(graph: Graph[Any, Int]): Double = {
    println("////////////////////")
    val community: Set[Long] = Set(236l, 186l, 88l, 213l)

    graph.mapTriplets(e => modularity(community, e)).edges.foreach(e => {
      if (e.attr != 0) println(e)
    })

    println("////////////////////")
    0.3
  }

  def modularity(set: Set[Long], edge: EdgeTriplet[Any, Int]): Long = {
    //    println(s"edgeTriplet: ${edge.toString}")

    //    if ((set.contains(edge.srcId) && !set.contains(edge.dstId)) || (!set.contains(edge.srcId) && set.contains(edge.dstId))) {
    //      (1 * ( graph.degrees.lookup(edge.srcId).head * graph.degrees.lookup(edge.dstId).head)) / (2 * edge_number)
    //    } else {
    //      0
    //    }
    1L
  }
}
