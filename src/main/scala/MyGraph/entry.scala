package MyGraph

import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

/**
  * Created by etrunon on 01/12/16.
  */
object entry {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyGraph").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val mygraph = new MyGraph(sc)

    val file = "Data/facebook-cleaned/total.csv"
    val graph = Graph.fromEdgeTuples(mygraph.getEdgeTuples(file), 0)

    //    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    //    println(s"MaxDegree: $maxInDegree")

    //    val cc = graph.connectedComponents().vertices
    //    println(s"ConnectecCOmponents: $cc")

    println("Beginning triangle counting")
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount()
    for (tri <- triCounts.vertices.take(10)) println(tri)

    println("Press any key to exit")
    StdIn.readLine()
  }

  //  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  //    if (a._2 > b._2) a else b
  //  }
}
