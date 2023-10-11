package com.lsc

import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory, statesTotal}
import NetGraphAlgebraDefs.{Action, GraphPerturbationAlgebra, NetGraph, NetModelAlgebra, NodeObject, simRank}
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{CreateLogger, NGSConstants}
import com.google.common.graph.*

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.ConfigFactory
import guru.nidi.graphviz.engine.Format
import org.slf4j.Logger

import java.net.{InetAddress, NetworkInterface, Socket}
import scala.util.{Failure, Success}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.parallel.ParSeq
import collection.JavaConverters.iterableAsScalaIterableConverter
import collection.parallel.CollectionConverters.ImmutableSeqIsParallelizable
import collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import collection.parallel.CollectionConverters.seqIsParallelizable
import collection.parallel.CollectionConverters.IterableIsParallelizable
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.io.File


object Main:
  val logger:Logger = CreateLogger(classOf[Main.type])
  val ipAddr: InetAddress = InetAddress.getLocalHost
  val hostName: String = ipAddr.getHostName
  val hostAddress: String = ipAddr.getHostAddress


  private class GraphMapper extends Mapper[Object,Text,Text,IntWritable] {
    /* Mapper for tasks
     * Input is a file containing a single line in the format "listOfNodes; listOfNodes; listOfEdges; listOfEdges"
     * Mapper reconstructs the two shards (original graph and perturbed graph) from the lists of nodes and edges
     * The simrank algorithm is called on to make comparisons between original subgraph and perturbed subgraph
     */
    override
    def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {
      val list = value.toString.split(";")(0)
      val listofnodes = list.split(",")

      val plist = value.toString.split(";")(1)
      val plistofnodes = plist.split(",")

      val edges = value.toString.split(";")(2).split(",")
      val pedges = value.toString.split(";")(3).split(",")

      val firstShard: MutableValueGraph[String, Int] = ValueGraphBuilder.directed.allowsSelfLoops(false).build
      val secondShard: MutableValueGraph[String, Int] = ValueGraphBuilder.directed.allowsSelfLoops(false).build

      listofnodes.foreach(node => firstShard.addNode(node))
      plistofnodes.foreach(node => secondShard.addNode(node))
      edges.foreach(node => firstShard.putEdgeValue(node.split(":")(0), node.split(":")(1), 1))
      pedges.foreach(node => secondShard.putEdgeValue(node.split(":")(0), node.split(":")(1), 1))

      NetGraphAlgebraDefs.simRank().simRank(firstShard, secondShard, context)
    }
  }

  private class GraphReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    /* Reducer for tasks
     * Input is the context that has been populated by the simrank algorithm with delta between graphs
     * The reducer merges the results from each mapper, eliminating duplicates and ordering the results
     * and writes them to output file
     */
    override
    def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={
      context.write(key, new IntWritable(1))
    }
  }

    def main(args: Array[String]): Unit =
    import scala.jdk.CollectionConverters.*
    val outGraphFileName = if args.isEmpty then NGSConstants.OUTPUTFILENAME else args(0).concat(NGSConstants.DEFOUTFILEEXT)
    val perturbedOutGraphFileName = outGraphFileName.concat(".perturbed")
    logger.info(s"Output graph file is $outputDirectory$outGraphFileName and its perturbed counterpart is $outputDirectory$perturbedOutGraphFileName")
    logger.info(s"The netgraphsim program is run at the host $hostName with the following IP addresses:")
    logger.info(ipAddr.getHostAddress)
    NetworkInterface.getNetworkInterfaces.asScala
        .flatMap(_.getInetAddresses.asScala)
        .filterNot(_.getHostAddress == ipAddr.getHostAddress)
        .filterNot(_.getHostAddress == "127.0.0.1")
        .filterNot(_.getHostAddress.contains(":"))
        .map(_.getHostAddress).toList.foreach(a => logger.info(a))

    val existingGraph = java.io.File(s"$outputDirectory$outGraphFileName").exists
    val g: Option[NetGraph] = if existingGraph then
      logger.warn(s"File $outputDirectory$outGraphFileName is located, loading it up. If you want a new generated graph please delete the existing file or change the file name.")
      NetGraph.load(fileName = s"$outputDirectory$outGraphFileName")
    else
      val config = ConfigFactory.load()
      logger.info("for the main entry")
      config.getConfig("NGSimulator").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      logger.info("for the NetModel entry")
      config.getConfig("NGSimulator").getConfig("NetModel").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      NetModelAlgebra()

    if g.isEmpty then logger.error("Failed to generate a graph. Exiting...")
    else
      logger.info(s"The original graph contains ${g.get.totalNodes} nodes and ${g.get.sm.edges().size()} edges; the configuration parameter specified ${NetModelAlgebra.statesTotal} nodes.")
      if !existingGraph then
        g.get.persist(fileName = outGraphFileName)
        logger.info(s"Generating DOT file for graph with ${g.get.totalNodes} nodes for visualization as $outputDirectory$outGraphFileName.dot")
        g.get.toDotVizFormat(name = s"Net Graph with ${g.get.totalNodes} nodes", dir = outputDirectory, fileName = outGraphFileName, outputImageFormat = Format.DOT)
        logger.info(s"A graph image file can be generated using the following command: sfdp -x -Goverlap=scale -Tpng $outputDirectory$outGraphFileName.dot > $outputDirectory$outGraphFileName.png")
      end if
      logger.info("Perturbing the original graph to create its modified counterpart...")
      val perturbation: GraphPerturbationAlgebra#GraphPerturbationTuple = GraphPerturbationAlgebra(g.get.copy)
      perturbation._1.persist(fileName = perturbedOutGraphFileName)

      logger.info(s"Generating DOT file for graph with ${perturbation._1.totalNodes} nodes for visualization as $outputDirectory$perturbedOutGraphFileName.dot")
      perturbation._1.toDotVizFormat(name = s"Perturbed Net Graph with ${perturbation._1.totalNodes} nodes", dir = outputDirectory, fileName = perturbedOutGraphFileName, outputImageFormat = Format.DOT)
      logger.info(s"A graph image file for the perturbed graph can be generated using the following command: sfdp -x -Goverlap=scale -Tpng $outputDirectory$perturbedOutGraphFileName.dot > $outputDirectory$perturbedOutGraphFileName.png")

      val modifications:ModificationRecord = perturbation._2
      GraphPerturbationAlgebra.persist(modifications, outputDirectory.concat(outGraphFileName.concat(".yaml"))) match
        case Left(value) => logger.error(s"Failed to save modifications in ${outputDirectory.concat(outGraphFileName.concat(".yaml"))} for reason $value")
        case Right(value) =>
          logger.info(s"Diff yaml file ${outputDirectory.concat(outGraphFileName.concat(".yaml"))} contains the delta between the original and the perturbed graphs.")
          logger.info(s"Done! Please check the content of the output directory $outputDirectory")

          /* Code written by Shreya Boyapati for Homework 1*/
          val perturbed = NetGraph.load(s"$outGraphFileName.perturbed")

          /* Create input directory for mapper input
           * Use date to keep the name unique and avoid accidental overwriting
           */
          val df = new SimpleDateFormat("dd-MM-yy-HH-mm-ss")
          val myDir = new File("/Users/shreyaboyapati/Downloads/NetGameSim-main/input" + df.format(new Date(System.currentTimeMillis())))
          myDir.mkdir()

          /* Pattern match original graph to get the NetGraph object
           * Use createShards class to split graph into shards - each shard of size 10
           * Error checking: if g is None rather than Some, warn the user
           */
          g match
            case Some(graph) =>
              NetGraphAlgebraDefs.createShards().shards(graph, perturbed, myDir)
            case None =>
              logger.warn("Graph is incorrect case")

          /* Driver code for map/reduce
           * Use GraphMapper and GraphReducer classes for the tasks
           * Input is the previously created directory which now contains files corresponding to created shards
           * Output will be contained in a single text file in the format "delta  1\n"
           */
          val conf = new Configuration()
          val job = new Job(conf, "simrank")
          job.setJarByClass(classOf[GraphMapper])
          job.setMapperClass(classOf[GraphMapper])
          job.setCombinerClass(classOf[GraphReducer])
          job.setReducerClass(classOf[GraphReducer])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[IntWritable])
          FileInputFormat.addInputPath(job, new Path(myDir.toString))
          val outputpath = s"$outputDirectory/" + df.format(new Date(System.currentTimeMillis()))
          FileOutputFormat.setOutputPath(job, new Path(outputpath))
          if (job.waitForCompletion(true)) 0 else 1

          logger.info("Map/Reduce output is available at " + outputpath + "/part-r-00000")
