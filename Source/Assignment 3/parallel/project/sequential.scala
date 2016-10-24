import java.time.LocalTime

import scala.annotation.tailrec
import scala.util.Random

import scalaz.stream._
import scalaz.concurrent.{Strategy, Task}

object MergeSort {
  def main(args: Array[String]): Unit = {
    val a: List[Int] = Random.shuffle((1 to 4).toList)
    val merged = mergeSort(a)(Strategy.DefaultStrategy).runLog.run // no problem
    //    val merged = mergeSort(a)(Strategy.Sequential).runLog.run // no good; livelock

    println(merged.mkString("\n"))
  }

  def mergeLists(l1: List[Int], l2: List[Int]): List[Int] = {
    println(s"merging $l1 and $l2 at ${LocalTime.now()} (< ${l1.length max l2.length} secs)")

    @tailrec def loop(l1: List[Int], l2: List[Int], merged: List[Int]): List[Int] =
      (l1, l2) match {
        case (a :: as, b :: bs) =>
          if (a < b)
            loop(as, l2, a :: merged)
          else
            loop(l1, bs, b :: merged)

        case (Nil, bs) => merged.reverse ++ bs
        case (as, Nil) => merged.reverse ++ as
      }

    loop(l1, l2, Nil)
  }

  def mergeSort(list: List[Int])(implicit S: Strategy): Process[Task, List[Int]] = {
    if (list.length < 2) Process.emit(list)
    else {
      val (l1, l2) = list.splitAt(list.length / 2)

      def sorted(l: List[Int]): Process[Task, List[Int]] =
        Process.emit(l).flatMap(mergeSort)

      (sorted(l1) wye sorted(l2))(wye.yipWith(mergeLists))
    }
  }
}