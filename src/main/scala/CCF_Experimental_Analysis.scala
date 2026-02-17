import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}

/**
 * Comprehensive Experimental Analysis comparing RDD and DataFrame implementations
 * of the Connected Components Finder (CCF) algorithm
 * 
 * This script:
 * 1. Generates graphs of increasing size
 * 2. Runs both RDD and DataFrame implementations
 * 3. Collects performance metrics (time, iterations, edges)
 * 4. Outputs comparison results
 */
object CCF_Experimental_Analysis {
  
  case class ExperimentResult(
    graphSize: Long,
    edgeCount: Long,
    implementation: String,
    iterations: Int,
    totalTime: Double,
    avgIterationTime: Double,
    componentCount: Long
  )
  
  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("CCF_Experimental_Analysis")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    import spark.implicits._
    
    try {
      println("\n" + "="*80)
      println("CCF ALGORITHM - EXPERIMENTAL ANALYSIS")
      println("Comparing RDD vs DataFrame Implementations")
      println("="*80)
      
      // Define graph sizes to test
      val graphSizes = Array(
        1000L,     // 1K nodes
        5000L,     // 5K nodes
        10000L,    // 10K nodes
        25000L,    // 25K nodes
        50000L,    // 50K nodes
        100000L    // 100K nodes
      )
      
      val avgDegree = 10  // Average degree per node
      val results = scala.collection.mutable.ArrayBuffer[ExperimentResult]()
      
      for (size <- graphSizes) {
        println("\n" + "="*80)
        println(s"Testing graph with $size nodes (avg degree: $avgDegree)")
        println("="*80)
        
        // Generate synthetic graph
        println("\nGenerating synthetic graph...")
        val (graphRDD, graphDF) = generateGraphs(spark, sc, size, avgDegree)
        
        val edgeCount = graphRDD.count()
        println(s"Generated graph with $edgeCount edges")
        
        // Test RDD implementation
        println("\n--- Testing RDD Implementation ---")
        val rddResult = runRDDExperiment(graphRDD, sc, size, edgeCount)
        results += rddResult
        
        // Test DataFrame implementation
        println("\n--- Testing DataFrame Implementation ---")
        val dfResult = runDataFrameExperiment(graphDF, spark, size, edgeCount)
        results += dfResult
        
        // Print comparison for this size
        printComparison(rddResult, dfResult)
        
        // Clean up
        graphRDD.unpersist()
        graphDF.unpersist()
      }
      
      // Print overall summary
      printOverallSummary(results.toSeq)
      
      // Save results to CSV
      saveResultsToCSV(results.toSeq, "results/csvs/ccf_experiment_results_scala.csv")
      
      println("\n" + "="*80)
      println("Experimental analysis complete!")
      println("="*80)
      
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Generate both RDD and DataFrame representations of the same graph
   */
  def generateGraphs(
    spark: SparkSession, 
    sc: org.apache.spark.SparkContext, 
    numNodes: Long, 
    avgDegree: Int
  ): (RDD[(Long, Long)], DataFrame) = {
    import spark.implicits._
    
    val numEdges = (numNodes * avgDegree / 2).toLong
    val seed = 42L  // Use same seed for reproducibility
    
    // Generate edges
    val edges = sc.parallelize(0L until numEdges).map { i =>
      val rng = new scala.util.Random(seed + i)
      val src = rng.nextLong().abs % numNodes
      val dst = rng.nextLong().abs % numNodes
      if (src != dst) (src, dst) else (src, (dst + 1) % numNodes)
    }.distinct()
    
    // Make undirected for RDD
    val reverseEdgesRDD = edges.map { case (src, dst) => (dst, src) }
    val graphRDD = edges.union(reverseEdgesRDD).distinct().cache()
    
    // Convert to DataFrame
    val edgesDF = edges.toDF("src", "dst")
    val reverseEdgesDF = edgesDF.select(
      col("dst").alias("src"),
      col("src").alias("dst")
    )
    val graphDF = edgesDF.union(reverseEdgesDF).distinct().cache()
    
    // Materialize both
    graphRDD.count()
    graphDF.count()
    
    (graphRDD, graphDF)
  }
  
  /**
   * Run experiment with RDD implementation
   */
  def runRDDExperiment(
    graph: RDD[(Long, Long)], 
    sc: org.apache.spark.SparkContext,
    size: Long,
    edgeCount: Long
  ): ExperimentResult = {
    var currentGraph = graph
    var iteration = 0
    var changed = true
    val iterationTimes = scala.collection.mutable.ArrayBuffer[Double]()
    
    val totalStartTime = System.currentTimeMillis()
    
    while (changed) {
      val iterStartTime = System.currentTimeMillis()
      iteration += 1
      
      // Group neighbors by node
      val grouped = currentGraph.groupByKey()
      
      // Propagate minimum IDs
      val updated = grouped.flatMap { case (node, neighbors) =>
        val neighborList = neighbors.toList
        
        if (neighborList.isEmpty) {
          Seq.empty[(Long, Long)]
        } else {
          val minId = (node :: neighborList).min
          
          if (minId < node) {
            val results = scala.collection.mutable.ArrayBuffer[(Long, Long)]()
            results += ((node, minId))
            
            for (n <- neighborList if n != minId) {
              results += ((n, minId))
            }
            results.toSeq
          } else {
            Seq.empty[(Long, Long)]
          }
        }
      }.distinct().cache()
      
      // Check for changes
      val diff = updated.subtract(currentGraph)
      changed = !diff.isEmpty()
      
      // Replace old graph
      if (iteration > 1) currentGraph.unpersist()
      currentGraph = updated
      
      val iterTime = (System.currentTimeMillis() - iterStartTime) / 1000.0
      iterationTimes += iterTime
      println(f"  Iteration $iteration: $iterTime%.2f seconds")
    }
    
    val totalTime = (System.currentTimeMillis() - totalStartTime) / 1000.0
    val avgIterTime = iterationTimes.sum / iterationTimes.length
    
    // Count components
    val componentCount = currentGraph.groupByKey().count()
    
    ExperimentResult(
      size, edgeCount, "RDD", iteration, totalTime, avgIterTime, componentCount
    )
  }
  
  /**
   * Run experiment with DataFrame implementation
   */
  def runDataFrameExperiment(
    graph: DataFrame,
    spark: SparkSession,
    size: Long,
    edgeCount: Long
  ): ExperimentResult = {
    import spark.implicits._
    
    var currentGraph = graph
    var iteration = 0
    var changed = true
    val iterationTimes = scala.collection.mutable.ArrayBuffer[Double]()
    
    val totalStartTime = System.currentTimeMillis()
    
    while (changed) {
      val iterStartTime = System.currentTimeMillis()
      iteration += 1
      
      // Find minimum neighbor for each node
      val minDF = currentGraph.groupBy("src")
        .agg(min("dst").alias("min_id"))
      
      // Create new edges where min_id < src
      val newDF = currentGraph.join(minDF, "src")
        .filter(col("min_id") < col("src"))
        .select(
          col("src"),
          col("min_id").alias("dst")
        )
        .distinct()
        .cache()
      
      // Check for changes
      val diff = newDF.except(currentGraph)
      changed = diff.count() > 0
      
      // Replace old graph
      if (iteration > 1) currentGraph.unpersist()
      currentGraph = newDF
      
      val iterTime = (System.currentTimeMillis() - iterStartTime) / 1000.0
      iterationTimes += iterTime
      println(f"  Iteration $iteration: $iterTime%.2f seconds")
    }
    
    val totalTime = (System.currentTimeMillis() - totalStartTime) / 1000.0
    val avgIterTime = iterationTimes.sum / iterationTimes.length
    
    // Count components
    val componentCount = currentGraph.groupBy("dst").count().count()
    
    ExperimentResult(
      size, edgeCount, "DataFrame", iteration, totalTime, avgIterTime, componentCount
    )
  }
  
  /**
   * Print comparison between RDD and DataFrame for a specific graph size
   */
  def printComparison(rddResult: ExperimentResult, dfResult: ExperimentResult): Unit = {
    println("\n--- Comparison ---")
    println(f"Graph Size: ${rddResult.graphSize}%,d nodes, ${rddResult.edgeCount}%,d edges")
    println(f"Components Found: ${rddResult.componentCount}%,d")
    println()
    println("%-30s %-15s %-15s %-15s".format("Metric", "RDD", "DataFrame", "Speedup"))
    println("-" * 75)
    println("%-30s %-15.2f %-15.2f %-15.2fx".format("Total Time (s)", rddResult.totalTime, dfResult.totalTime, rddResult.totalTime/dfResult.totalTime))
    println("%-30s %-15d %-15d %-15.2fx".format("Iterations", rddResult.iterations, dfResult.iterations, rddResult.iterations.toDouble/dfResult.iterations))
    println("%-30s %-15.2f %-15.2f %-15.2fx".format("Avg Iteration Time (s)", rddResult.avgIterationTime, dfResult.avgIterationTime, rddResult.avgIterationTime/dfResult.avgIterationTime))
  }
  
  /**
   * Print overall summary of all experiments
   */
  def printOverallSummary(results: Seq[ExperimentResult]): Unit = {
    println("\n" + "="*80)
    println("OVERALL SUMMARY")
    println("="*80)
    
    val rddResults = results.filter(_.implementation == "RDD")
    val dfResults = results.filter(_.implementation == "DataFrame")
    
    println("\n%-12s %-12s %-12s %-12s %-12s %-12s %-12s".format("Size", "Edges", "Impl", "Iterations", "Total(s)", "Avg/Iter(s)", "Components"))
    println("-" * 84)
    
    for (i <- rddResults.indices) {
      val rdd = rddResults(i)
      val df = dfResults(i)
      
      println("%,12d %,12d %-12s %-12d %-12.2f %-12.2f %,12d".format(rdd.graphSize, rdd.edgeCount, "RDD", rdd.iterations, rdd.totalTime, rdd.avgIterationTime, rdd.componentCount))
      println("%,12d %,12d %-12s %-12d %-12.2f %-12.2f %,12d".format(df.graphSize, df.edgeCount, "DataFrame", df.iterations, df.totalTime, df.avgIterationTime, df.componentCount))
      
      val speedup = rdd.totalTime / df.totalTime
      val winner = if (speedup > 1) "DataFrame" else "RDD"
      println("%-12s %-12s %-12s %-12s %-12.2fx %-12s (%s faster)".format("", "", "Speedup:", "", speedup, "", winner))
      println()
    }
    
    // Calculate average speedup
    val avgSpeedup = rddResults.zip(dfResults).map { case (rdd, df) =>
      rdd.totalTime / df.totalTime
    }.sum / rddResults.length
    
    println(f"Average Speedup (DataFrame vs RDD): ${avgSpeedup}%.2fx")
  }
  
  /**
   * Save results to CSV file
   */
  def saveResultsToCSV(results: Seq[ExperimentResult], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    
    try {
      // Write header
      pw.println("GraphSize,EdgeCount,Implementation,Iterations,TotalTime,AvgIterationTime,ComponentCount")
      
      // Write data
      results.foreach { r =>
        pw.println(s"${r.graphSize},${r.edgeCount},${r.implementation},${r.iterations},${r.totalTime},${r.avgIterationTime},${r.componentCount}")
      }
      
      println(s"\nResults saved to: $filename")
    } finally {
      pw.close()
    }
  }
}
