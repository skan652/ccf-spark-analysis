import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Connected Components Finder (CCF) Algorithm - DataFrame Implementation in Scala
 * Based on the paper: https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf
 * 
 * This implementation uses Spark SQL DataFrames for potentially better optimization
 * through Catalyst query optimizer and Tungsten execution engine.
 */
object CCF_DataFrame {
  
  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("CCF_WebGoogle_DataFrame")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    try {
      // Read and process input file
      val inputPath = if (args.length > 0) args(0) else "web-Google.txt"
      println(s"Reading graph from: $inputPath")
      
      // Load edges from file
      val rawData = spark.read.text(inputPath)
      
      val edgesDF = rawData
        .filter(!col("value").startsWith("#"))
        .select(
          split(col("value"), "\\s+").getItem(0).cast(LongType).alias("src"),
          split(col("value"), "\\s+").getItem(1).cast(LongType).alias("dst")
        )
        .filter(col("src").isNotNull && col("dst").isNotNull)
      
      // Convert to undirected graph
      val edgesRev = edgesDF.select(
        col("dst").alias("src"),
        col("src").alias("dst")
      )
      
      var graph = edgesDF.union(edgesRev).distinct().cache()
      
      println(s"Initial edges: ${graph.count()}")
      
      // Run CCF algorithm
      val (finalGraph, iterations) = runCCF(graph, spark)
      
      // Count components
      val components = finalGraph.groupBy("dst").count()
      println(s"\nTotal components: ${components.count()}")
      println(s"Finished in $iterations iterations")
      
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Run the CCF algorithm on the graph using DataFrames
   * 
   * @param initialGraph Initial edge DataFrame
   * @param spark SparkSession
   * @return Tuple of (final graph, number of iterations)
   */
  def runCCF(initialGraph: DataFrame, spark: SparkSession): (DataFrame, Int) = {
    import spark.implicits._
    
    var graph = initialGraph
    var iteration = 0
    var changed = true
    
    while (changed) {
      val startTime = System.currentTimeMillis()
      iteration += 1
      println(s"\nIteration $iteration")
      
      // Find minimum neighbor for each node
      val minDF = graph.groupBy("src")
        .agg(min("dst").alias("min_id"))
      
      // Create new edges where min_id < src
      val newDF = graph.join(minDF, "src")
        .filter(col("min_id") < col("src"))
        .select(
          col("src"),
          col("min_id").alias("dst")
        )
        .distinct()
        .cache()
      
      // Check for changes
      val diff = newDF.except(graph)
      changed = diff.count() > 0
      
      // Replace old graph
      graph.unpersist()
      graph = newDF
      
      val elapsedTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"Iteration time: $elapsedTime%.2f seconds")
    }
    
    (graph, iteration)
  }
  
  /**
   * Generate a synthetic graph for testing using DataFrames
   * 
   * @param spark SparkSession
   * @param numNodes Number of nodes
   * @param avgDegree Average degree per node
   * @return DataFrame of edges
   */
  def generateSyntheticGraph(spark: SparkSession, numNodes: Long, avgDegree: Int): DataFrame = {
    import spark.implicits._
    
    val numEdges = (numNodes * avgDegree / 2).toLong
    
    val edges = spark.sparkContext
      .parallelize(0L until numEdges)
      .map { _ =>
        val src = scala.util.Random.nextLong().abs % numNodes
        val dst = scala.util.Random.nextLong().abs % numNodes
        if (src != dst) (src, dst) else (src, (dst + 1) % numNodes)
      }
      .toDF("src", "dst")
      .distinct()
    
    // Make undirected
    val reverseEdges = edges.select(
      col("dst").alias("src"),
      col("src").alias("dst")
    )
    
    edges.union(reverseEdges).distinct()
  }
}
