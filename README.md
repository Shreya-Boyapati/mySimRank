SimRank (mySimRank) Algorithm
=======================
#### The goal of this algorithm is to compare delta between two graphs that are computed by NetGameSim module

Overview
---
* NetGameSim can be found at [https://github.com/0x1DOCD00D/NetGameSim]
* Key changes made are found in
  1. Main.scala
  2. createShards.scala
  3. simRank.scala
* Main.scala contains driver code of the mapreduce job and gives the final result
* createShards.scala contains the method to create shards of two NetGraphs and write them to a text file
* simRank.scala contains the simrank algorithm that detects the delta between two given graphs


Installing, Compiling and Running mySimRank 
===
* Once all prerequisites are met, clone [mySimRank](https://github.com/Shreya-Boyapati/mySimRank) using the command ```git clone```;
* Once the repository is cloned, assuming its local path is the root directory ```/path/to/the/cloned/mySimRank``` open a terminal window and switch to the directory ```cd /path/to/the/cloned/mySimRank```;
* Build and run the project using the command ```sbt clean compile run```
* Alternatively, you can load the project into IntelliJ and compile and run it using the main entry point in [Main.scala](src/main/scala/Main.scala);
* You should make sure that the Java version that is used to compile this project matches the JVM version that is used to run the generated program jar from the command line, otherwise you may receive an error.

Running mySimRank results in many log messages showing the progress of the execution, the last log entries can look like the following.
```log
...
File Input Format Counters 
		Bytes Read=13223
	File Output Format Counters 
		Bytes Written=62970
22:36:41.414 [main] INFO  - Map/Reduce output is available at /Users/shreyaboyapati/Downloads/NetGameSim-main/outputs//10-10-23-22-36-39/part-r-00000

```
The output file that is given by the last line of the logger can be opened to find the results of the simrank algorithm conducted by mapreduce.


**Testing**
===
Testing on mySimRank was done using [Scalatest]([url](https://github.com/scalatest/)) dependency

Maintenance notes
===
This program is written by Shreya Boyapati






