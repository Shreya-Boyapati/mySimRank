package NetGraphAlgebraDefs

import Utilz.CreateLogger
import com.google.common.graph.MutableValueGraph
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.parallel.CollectionConverters.{ImmutableIterableIsParallelizable, ImmutableSeqIsParallelizable, IterableIsParallelizable, seqIsParallelizable}
import scala.collection.parallel.ParSeq

class simRank {
  val logger: Logger = CreateLogger(this.getClass)
  /* Compares original and perturbed graph to determine the delta between the two
   * Inspiration was taken from the NetGraph.compare() method
   * Algorithm traverses through the list of nodes of the original graph
   * and compares it to the nodes of the perturbed graph to determine which have been removed.
   * The same is done with the two flipped to determine which nodes have been added.
   * The process is then repeated on the lists of edges to determine which have been added/removed.
   * Two versions of the simRank are given: with context (used by mapreduce) and without
   */
  def simRank(orig: MutableValueGraph[String, Int], perturbed: MutableValueGraph[String, Int]): Int = {
    val thisNodes = orig.nodes().asScala.toList.par
    val thatNodes = perturbed.nodes().asScala.toList.par
    val thisEdges = orig.edges().asScala.toList
    val thatEdges = perturbed.edges().asScala.toList

    val one = new IntWritable(1)
    var count:Int = 0

    thisNodes.foldLeft(0) {
      case (acc, node) => if thatNodes.exists(_ == node) then acc else
        logger.info(s"Node ${node} has been removed from original graph")
        count = count + 1
        acc + 1
    } + thatNodes.foldLeft(0) {
      case (acc, node) => if thisNodes.exists(_ == node) then acc else
        logger.info(s"Node ${node} has been added to original graph")
        count = count + 1
        acc + 1
    } + thisEdges.foldLeft(0) { case (acc, e) =>
      val found = thatEdges.find(n => n == e)
      if found.isDefined && e.nodeU() == thatEdges.find(n => n == e).get.nodeU() &&
        e.nodeV() == thatEdges.find(n => n == e).get.nodeV()
      then acc else
        logger.info(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been removed from original graph")
        count = count + 1
        acc + 1
    } + thatEdges.foldLeft(0) { case (acc, e) =>
      val found = thisEdges.find(n => n == e)
      if found.isDefined && e.nodeU() == thisEdges.find(n => n == e).get.nodeU() &&
        e.nodeV() == thisEdges.find(n => n == e).get.nodeV()
      then acc else
        logger.info(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been added to original graph")
        count = count + 1
        acc + 1

      return count
    }
  }

  def simRank(orig: MutableValueGraph[String, Int], perturbed: MutableValueGraph[String, Int], context: Mapper[Object, Text, Text, IntWritable]#Context): Int = {
    val thisNodes = orig.nodes().asScala.toList.par
    val thatNodes = perturbed.nodes().asScala.toList.par
    val thisEdges = orig.edges().asScala.toList
    val thatEdges = perturbed.edges().asScala.toList

    val one = new IntWritable(1)
    val count = 0

    thisNodes.foldLeft(0) {
      case (acc, node) => if thatNodes.exists(_ == node) then acc else
        logger.info(s"Node ${node} has been removed from original graph")
        context.write(new Text(s"Node ${node} has been removed from original graph"), one)
        acc + 1
    } + thatNodes.foldLeft(0) {
      case (acc, node) => if thisNodes.exists(_ == node) then acc else
        logger.info(s"Node ${node} has been added to original graph")
        context.write(new Text(s"Node ${node} has been added to original graph"), one)
        acc + 1
    } + thisEdges.foldLeft(0) { case (acc, e) =>
      val found = thatEdges.find(n => n == e)
      if found.isDefined && e.nodeU() == thatEdges.find(n => n == e).get.nodeU() &&
        e.nodeV() == thatEdges.find(n => n == e).get.nodeV()
      then acc else
        logger.info(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been removed from original graph")
        context.write(new Text(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been removed from original graph"), one)
        acc + 1
    } + thatEdges.foldLeft(0) { case (acc, e) =>
      val found = thisEdges.find(n => n == e)
      if found.isDefined && e.nodeU() == thisEdges.find(n => n == e).get.nodeU() &&
        e.nodeV() == thisEdges.find(n => n == e).get.nodeV()
      then acc else
        logger.info(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been added to original graph")
        context.write(new Text(s"Edge ${e.nodeU()} -> ${e.nodeV()} has been added to original graph"), one)
        acc + 1

      return acc
    }
  }
}
