import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 28/11/16.
  */

object visits {

  def main(args: Array[String]) {

    val graph = List((1, 2), (2, 3), (2, 4), (3, 4), (1, 5), (5, 6), (5, 7), (6, 7), (3, 8))

    // graph.foreach(println)

    val starting_node = 1

    dfs(graph, starting_node, ListBuffer()).foreach(println)
    bfs(graph, ListBuffer(starting_node), ListBuffer())

  }

  def dfs(graph: List[(Int, Int)], node: Int, seen: ListBuffer[Int]): List[Int] = {

    println("===DFS===\n")
    seen += node
    println("Seen:")
    seen.foreach(println)

    for (edge <- graph)
      if (edge._1 == node && !seen.contains(edge._2))
        dfs(graph, edge._2, seen)
      else if (edge._2 == node && !seen.contains(edge._1))
        dfs(graph, edge._1, seen)

    seen.toList
  }

  def bfs(graph: List[(Int, Int)], node: ListBuffer[Int], seen: ListBuffer[Int]): List[Int] = {

    println("===BFS===\n")
    println(node)

    var modified = false
    var tmp_node = ListBuffer[Int]

    for (edge <- graph)
      if (node.contains(edge._1) && !node.contains(edge._2)) {
        tmp_node.+= edge._2
        modified = true
      } else if (node.contains(edge._2) && !node.contains(edge._1)) {
        tmp_node.+= edge._1
        modified = true
      }

    if (modified) bfs(graph, tmp_node, seen)
//      seen.toList
    List(1)
  }

  def test_print(couple: (Int, Int), node: Int): Unit = {
    println("Start of test_print")
    if (couple._1 == node || couple._2 == node) {
      println(couple)
    }
  }
}