package NetGraphAlgebraDefs

import NetGraphAlgebraDefs.NetModelAlgebra.{outputDirectory, statesTotal}
import NetGraphAlgebraDefs.simRank
import Utilz.ConfigReader.getConfigEntry
import Utilz.{CreateLogger, NGSConstants}
import Utilz.NGSConstants.{DEFAULTEDGEPROBABILITY, EDGEPROBABILITY}
import com.google.common.graph.{MutableValueGraph, ValueGraphBuilder}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.slf4j.Logger

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

class testShards extends AnyFunSuite with MockitoSugar {
  val logger: Logger = CreateLogger(this.getClass)
  logger.info("Checks that correct number of shards are created")
  test("splitshards"){
    import Utilz.ConfigReader
    val crMock = mock[NetModel]
    when(crMock.generateModel()).thenReturn(Option(NetGraph(null, null)))

    val outGraphFileName = NGSConstants.OUTPUTFILENAME.concat("test.perturbed")

    val g: Option[NetGraph] =
      val config = ConfigFactory.load()
      config.getConfig("NGSimulator").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      config.getConfig("NGSimulator").getConfig("NetModel").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      NetModelAlgebra()

    val myDir = new File("/Users/shreyaboyapati/Downloads/NetGameSim-main/test")
    myDir.mkdir()

    g match
      case Some(graph) =>
        val num = NetGraphAlgebraDefs.createShards().shards(graph, g, myDir)
        assert(num == (statesTotal-10))
      case None =>
        logger.warn("Graph is incorrect case")
  }
}
