
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphframes._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SocialNetworkGraphAnalysis {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Correct usage: Program_Name inputFile outputFile")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkSession.builder()
      .appName("Social Network Graph Analysis")
      .getOrCreate()

    var output = ""

    val edgesData = spark.read
      .option("inferSchema","true")
      .option("sep","\t")
      .csv(inputFile)
      .toDF("src","dst")

    val vertices = edgesData.withColumnRenamed("src","id").distinct()
    val property_graph = GraphFrame(vertices, edgesData)
    output=output+"The property graph is created\n"

    //a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each
    val outDegreeTop5 = property_graph.outDegrees
    val ans1 = outDegreeTop5.orderBy(desc("outDegree")).limit(5)
    output=output+"Top 5 outDregee nodes are:\n"+DFtoString(ans1)+"\n\n"

    //b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges in each
    val inDegreeTop5 = property_graph.inDegrees.orderBy(desc("inDegree"))
    val ans2 = inDegreeTop5.limit(5)
    output=output+"Top 5 inDregee nodes are:\n"+DFtoString(ans2)+"\n\n"

    //c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values. You are free to define the threshold parameter.
    val ranks = property_graph.pageRank.resetProbability(0.15).maxIter(10).run()
    val ans3 = ranks.vertices.orderBy(desc("pagerank")).select("id","pagerank").distinct.limit(5)
    output=output+"Top 5 nodes with the highest PageRank values are:\n"+DFtoString(ans3)+"\n\n"

    //d. Run the connected components algorithm on it and find the top 5 components with the largest number of nodes.
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val property_graph_ssc = property_graph.stronglyConnectedComponents.maxIter(3).run()
    val ans4_1 = property_graph_ssc.groupBy("component").count().orderBy(desc("count")).limit(5)
    output=output+"Top 5 components with the largest number of nodes are(strongly connected commponents algorithm):\n"+DFtoString(ans4_1)+"\n\n"

    val cc = property_graph.connectedComponents.run()
    val ans4_2 = cc.groupBy("component").count().orderBy(desc("count")).limit(5)
    output=output+"Top 5 components with the largest number of nodes are(connected components algorithm):\n"+DFtoString(ans4_2)+"\n\n"

    //e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices.
    val minGraph = GraphFrame(vertices,edgesData.sample(false, 0.1))
    val triCounts = minGraph.triangleCount.run()
    val ans5 = triCounts.select("id","count").distinct().orderBy(desc("count")).limit(5)
    output=output+"Top 5 vertices with the largest triangle count are:\n"+DFtoString(ans5)+"\n\n"

    val sc = spark.sparkContext
    var outputRdd: RDD[String] = sc.parallelize(List(output))
    outputRdd.coalesce(1, true).saveAsTextFile(outputFile)
  }
  def DFtoString(df1: DataFrame):String = {
    var ans =""
    ans = ans+df1.columns.mkString(",")+"\n"
    val df = df1.collect()
    df.foreach ( (row:Row) => ans=ans+row.mkString(",")+"\n")
    ans
  }
}
