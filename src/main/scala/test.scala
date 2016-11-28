import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object test {

  def main(args: Array[String]) {

    if (args.length != 1) {
      System.err.println("Correct usage with one param: 'path/to/edges'")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("GraphTest")
      //        .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
      .setMaster("local")

    val sc = new SparkContext(conf)

    val edges: RDD[Edge[String]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split("\t")
        Edge(fields(0).toLong, fields(2).toLong, fields(1))
      }

    val graph: Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

    println(s"num edges = ${graph.numEdges}")
    println(s"num vertices = ${graph.numVertices}")



//    let's visit the graph

    //    val shortest = ShortestPaths.run(graph, Seq(4028))

//    val onePath = shortest
//        .vertices
//        .filter({case(vId, _) => vId == 0})
//          .first()
//          ._2
//        .get(4028)

//    println(onePath)

  }
}