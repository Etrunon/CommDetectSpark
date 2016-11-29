import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 28/11/16.
  */

object visits {

  def main(args: Array[String]) {

    val graph = List((1, 2), (2, 3), (2, 4), (3, 4), (1, 5), (5, 6), (5, 7), (6, 7), (3, 8), (8, 9), (9, 10))

    // graph.foreach(println)

    val starting_node = 1

    dfs(graph, starting_node, ListBuffer()).foreach(println)
    bfs(graph, ListBuffer(starting_node))

  }

  def dfs(graph: List[(Int, Int)], node: Int, seen: ListBuffer[Int]): List[Int] = {

    seen += node
    println("===DFS===\n")
    println(s"Seen: $seen")

    for (edge <- graph)
      if (edge._1 == node && !seen.contains(edge._2))
        dfs(graph, edge._2, seen)
      else if (edge._2 == node && !seen.contains(edge._1))
        dfs(graph, edge._1, seen)

    seen.toList
  }

  def bfs(graph: List[(Int, Int)], node: ListBuffer[Int]): List[Int] = {

    println("===BFS===\n")
    println(s"Node: $node")
    var tmp_list: ListBuffer[Int] = ListBuffer()

    //Iterate on the node list and add it to tmp
    node.foreach(tmp_list +=)
    println(s"Tmplist: $tmp_list")

    for (edge <- graph) {
      val res = reachable_edge(node, edge)
      if (res != -1) tmp_list += res
    }

    if (node == tmp_list) tmp_list.toList
    else {
      for (vertex <- tmp_list) {
        if (!node.contains(vertex)) node += vertex
      }

      bfs(graph, node)
    }
  }

  def reachable_edge(node: ListBuffer[Int], edge: (Int, Int)): Int = {
    if (node.contains(edge._1) && !node.contains(edge._2)) {
      edge._2
    }
    else if (node.contains(edge._2) && !node.contains(edge._1)) {
      edge._1
    }
    else -1
  }

  def test_print(couple: (Int, Int), node: Int): Unit = {
    println("Start of test_print")
    if (couple._1 == node || couple._2 == node) {
      println(couple)
    }
  }
}