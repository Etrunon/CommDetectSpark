import java.io.FileNotFoundException

import scala.io.{Source, StdIn}

/**
  * Created by etrunon on 29/11/16.
  */
object linereader {
  def main(args: Array[String]): Unit = {
    println("Type filepath of the file to count")
    val file = StdIn.readLine()

    println(s"Opening $file")
    try {

      val lines = Source.fromFile(file).getLines().toList
      val maxLen = (Int.MinValue /: lines) ((max, line) => if (max > line.length) max else line.length)
      for (line <- lines) println(s" ${line.length}${" " * (maxLen.toString.length - line.length)} | $line")

    } catch {
      case fe: FileNotFoundException => println("Sorry, file not found")
    }
  }
}
