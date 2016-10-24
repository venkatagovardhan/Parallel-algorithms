import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Random
import akka.actor.{ActorSystem, Actor, Props}
import akka.pattern.ask
import akka.util.Timeout
import akka.routing.RoundRobinPool


object MergeSort extends App {

  case class Sort(items: Vector[Int])
  case class Merge(left: Vector[Int], right: Vector[Int])

  class Worker extends Actor {
    def receive = {
      case Sort(items) => sender() ! sort(items)
      case Merge(left, right) => sender() ! merge(left, right)
    }
  }

  def merge(left: Vector[Int], right: Vector[Int]): Vector[Int] = {
    var leftIndex = 0
    var rightIndex = 0
    var merged = Vector[Int]()
    while (leftIndex < left.length && rightIndex < right.length) {
      if (left(leftIndex) <= right(rightIndex)) {
        merged :+= left(leftIndex)
        leftIndex += 1
      } else {
        merged :+= right(rightIndex)
        rightIndex += 1
      }
    }
    if (leftIndex == left.length) {
      merged ++ right.slice(rightIndex, right.length)
    } else {
      merged ++ left.slice(leftIndex, left.length)
    }
  }

  def sort(items: Vector[Int]): Vector[Int] = {
    items match {
      case Vector(_) => items
      case _ => {
        val (left, right) = items.splitAt(items.length / 2)
        merge(sort(left), sort(right))
      }
    }
  }

  def run(items: Int, workers: Int) = {

    val unsorted = Vector.fill(items)(Random.nextInt)
    val blockSize = (items / workers.toFloat).ceil.toInt
    val timeout_ = items seconds
    implicit val timeout: Timeout = timeout_
    var blocks = unsorted.grouped(blockSize).toList
    val pool = system.actorOf(RoundRobinPool(workers).props(Props[Worker]))
    val start = System.nanoTime

    // First phase - split items into partitions and sort
    // each one on a separate actor .
    var futures = Future.traverse(blocks)(items => {
      pool ? new Sort(items)
    })
    blocks = Await.result(futures, timeout_).asInstanceOf[List[Vector[Int]]]

    // Second phase - merge pairs of partitions back together until
    // there's only one remaining.
    while (blocks.length > 1) {
      if (blocks.length % 2 == 1) {
        blocks = blocks :+ Vector[Int]()
      }
      futures = Future.traverse(blocks.grouped(2).toList)(items => {
        pool ? new Merge(items(0), items(1))
      })
      blocks = Await.result(futures, timeout_).asInstanceOf[List[Vector[Int]]]
    }

    val end = (System.nanoTime - start) / 1e7
    if (unsorted.sorted.mkString != blocks(0).mkString) {
      println("Invalid sort")
    } else {
      println(s"Sorted $items items with $workers workers in $end msecs")
    }

  }

  val system = ActorSystem()
  (1 to 4).foreach(_ => run(1000000, 4))
  system.shutdown()

}