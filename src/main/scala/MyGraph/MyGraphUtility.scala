package MyGraph

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by etrunon on 01/12/16.
  */
class MyGraphUtility(sc: SparkContext) {

  /** *
    * This method read the edges saved in the csv file and returns the RDD with those edges.
    * The CSV must have the form 'srcId\t Weight \t dstId'
    *
    * @param file path of the input file
    * @return RDD with edges
    */
  def readEdges(file: String): RDD[Edge[Int]] = {

    val lines = Source.fromFile(file).getLines()
    val results = new ListBuffer[Edge[Int]]()

    (results /: lines) ((res: ListBuffer[Edge[Int]], line: String) => {
      val spline = line.split("\t")
      res += new Edge(spline.apply(0).toInt, spline.apply(2).toInt, 0)
    })

    val ret: RDD[Edge[Int]] = sc.parallelize(results)
    ret
  }

  def getEdgeTuples(file: String): RDD[(VertexId, VertexId)] = {
    val read_file = sc.textFile(file)

    read_file.map(line => line.split("\t"))
      .map(line => (line(0).toLong, line(2).toLong))
  }
}