package NetGraphAlgebraDefs

import NetGraphAlgebraDefs.simRank
import Utilz.ConfigReader.getConfigEntry
import Utilz.CreateLogger
import Utilz.NGSConstants.{DEFAULTEDGEPROBABILITY, EDGEPROBABILITY}
import com.google.common.graph.{MutableValueGraph, ValueGraphBuilder}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.slf4j.Logger

class removeEdgeTest extends AnyFunSuite {
  val logger: Logger = CreateLogger(this.getClass)
  logger.info("Checks there is one difference when edge is removed")
  test("onechange") {
    val firstShard: MutableValueGraph[String, Int] = ValueGraphBuilder.directed.allowsSelfLoops(false).build
    val secondShard: MutableValueGraph[String, Int] = ValueGraphBuilder.directed.allowsSelfLoops(false).build

    List("1", "2", "3", "4", "5", "6", "7", "8", "9", "10").foreach(node => firstShard.addNode(node))
    List("1", "2", "3", "4", "5", "6", "7", "8", "9", "10").foreach(node => secondShard.addNode(node))

    firstShard.putEdgeValue("1", "2", 1)
    firstShard.putEdgeValue("5", "6", 1)
    firstShard.putEdgeValue("7", "9", 1)

    secondShard.putEdgeValue("1", "2", 1)
    secondShard.putEdgeValue("5", "6", 1)

    val result = NetGraphAlgebraDefs.simRank().simRank(firstShard, secondShard)
    assert(result == 1)
  }
}
