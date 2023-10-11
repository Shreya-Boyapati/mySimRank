package NetGraphAlgebraDefs

import NetGraphAlgebraDefs.NetModelAlgebra.statesTotal
import com.google.common.graph.Graphs

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.parallel.ParSeq
import collection.JavaConverters.iterableAsScalaIterableConverter
import collection.parallel.CollectionConverters.ImmutableSeqIsParallelizable
import collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import collection.parallel.CollectionConverters.seqIsParallelizable
import collection.parallel.CollectionConverters.IterableIsParallelizable
import collection.JavaConverters.setAsJavaSetConverter
import collection.JavaConverters.asJavaIterableConverter

class createShards() {
  /* Create shards from the graphs that are passed in
   * The inducedSubgraph method from google guava is used to create subgraphs
   * and the resulting shards are written into a file to be read by the mapper.
   * The contents of the file are formatted as four semi-colon separated lists.
   * Each shard is 10 nodes large, which ensures speedy processing for the mapper.
   */
  def shards(graph: NetGraph, perturbed: Option[NetGraph], myDir:File): Int = {
    val i = 0
    var j = 0
    for (i <- 1 to statesTotal / 10) {
      j = (i * 10) - 10
      val nodesForShard = graph.sm.nodes().asScala.toList slice(j, i * 10)
      val persistedNodes = perturbed.head.sm.nodes().asScala.toList slice(j, i * 10)
      val firstShard = Graphs.inducedSubgraph(graph.sm, nodesForShard.toSet.asJava)
      val secondShard = Graphs.inducedSubgraph(perturbed.head.sm, persistedNodes.toSet.asJava)

      val filepath = myDir.toString + "/" + i + "input.txt"
      var content = ""
      nodesForShard.foreach(node => content = content + node.id + ",")
      content = content + ";"
      persistedNodes.foreach(node => content = content + node.id + ",")
      content = content + ";"
      nodesForShard.foreach(node => node.childrenObjects.foreach(child => content = content + node.id + ":" + child.id + ","))
      content = content + ";"
      persistedNodes.foreach(node => node.childrenObjects.foreach(child => content = content + node.id + ":" + child.id + ","))

      val contentBytes = content.getBytes(StandardCharsets.UTF_8)
      Files.write(Paths.get(filepath), contentBytes)
    }
    j
  }
}
