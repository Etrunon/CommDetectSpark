package MyGraph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph

import scala.io.StdIn

/**
  * Created by etrunon on 01/12/16.
  */
object entry {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyGraph").setMaster("local")

    val sc = new SparkContext(conf)
    val mygraph = new MyGraph(sc)

    val file = "Data/facebook-cleaned/total.csv"
    val graph = Graph.fromEdgeTuples(mygraph.getEdgeTuples(file), 0)


    println("Press any key to exit")
    StdIn.readLine()

  }
}
