import org.apache.spark.sql.SparkSession

/**
 * Unified Main Runner for CCF Algorithm Experiments
 * 
 * This main class provides a menu-driven interface to run:
 * 1. RDD implementation on a specific graph file
 * 2. DataFrame implementation on a specific graph file
 * 3. Full experimental analysis (RDD vs DataFrame comparison)
 * 4. All of the above
 */
object CCF_Main {
  
  def main(args: Array[String]): Unit = {
    println("\n" + "="*80)
    println("CCF ALGORITHM - Connected Components Finder")
    println("PySpark Implementation: RDD vs DataFrame Comparison")
    println("="*80)
    
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("CCF_Main_Runner")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    try {
      // Determine which mode to run
      val mode = if (args.length > 0) args(0) else "all"
      
      mode.toLowerCase match {
        case "rdd" =>
          println("\n>>> Running RDD Implementation <<<\n")
          runRDDImplementation(args.drop(1), sc)
          
        case "dataframe" | "df" =>
          println("\n>>> Running DataFrame Implementation <<<\n")
          runDataFrameImplementation(args.drop(1), spark)
          
        case "experiment" | "exp" =>
          println("\n>>> Running Experimental Analysis <<<\n")
          runExperimentalAnalysis(spark, sc)
          
        case "all" | _ =>
          println("\n>>> Running Complete Analysis Suite <<<\n")
          runCompleteAnalysis(args.drop(1), spark, sc)
      }
      
      println("\n" + "="*80)
      println("EXECUTION COMPLETE")
      println("="*80)
      
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Run RDD implementation on input file
   */
  def runRDDImplementation(args: Array[String], sc: org.apache.spark.SparkContext): Unit = {
    val inputPath = if (args.length > 0) args(0) else {
      println("No input file specified. Running on synthetic graph...")
      return runSyntheticRDD(sc)
    }
    
    println(s"Reading graph from: $inputPath")
    
    // Load edges
    val rawEdges = sc.textFile(inputPath)
      .filter(!_.startsWith("#"))
      .map { line =>
        val parts = line.trim.split("\\s+")
        if (parts.length >= 2) {
          try {
            Some((parts(0).toLong, parts(1).toLong))
          } catch {
            case _: NumberFormatException => None
          }
        } else None
      }
      .filter(_.isDefined)
      .map(_.get)
    
    // Convert to undirected
    val reverseEdges = rawEdges.map { case (src, dst) => (dst, src) }
    val graph = rawEdges.union(reverseEdges).distinct().cache()
    
    println(s"Initial edges: ${graph.count()}")
    
    // Run CCF
    val startTime = System.currentTimeMillis()
    val (finalGraph, iterations) = CCF_RDD.runCCF(graph, sc)
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    // Count components
    val components = finalGraph.groupByKey().count()
    
    println(s"\nResults:")
    println(s"  Total components: $components")
    println(s"  Iterations: $iterations")
    println(f"  Total time: $totalTime%.2f seconds")
    println(f"  Avg time per iteration: ${totalTime/iterations}%.2f seconds")
  }
  
  /**
   * Run RDD on synthetic graph
   */
  def runSyntheticRDD(sc: org.apache.spark.SparkContext): Unit = {
    println("Generating synthetic graph (10,000 nodes, avg degree 10)...")
    val graph = CCF_RDD.generateSyntheticGraph(sc, 10000, 10).cache()
    val edgeCount = graph.count()
    println(s"Generated: $edgeCount edges")
    
    val startTime = System.currentTimeMillis()
    val (_, iterations) = CCF_RDD.runCCF(graph, sc)
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    println(f"\nCompleted in $iterations iterations, ${totalTime}%.2f seconds")
  }
  
  /**
   * Run DataFrame implementation on input file
   */
  def runDataFrameImplementation(args: Array[String], spark: SparkSession): Unit = {
    import spark.implicits._
    
    val inputPath = if (args.length > 0) args(0) else {
      println("No input file specified. Running on synthetic graph...")
      return runSyntheticDataFrame(spark)
    }
    
    println(s"Reading graph from: $inputPath")
    
    // Load edges
    val rawData = spark.read.text(inputPath)
    
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    
    val edgesDF = rawData
      .filter(!col("value").startsWith("#"))
      .select(
        split(col("value"), "\\s+").getItem(0).cast(LongType).alias("src"),
        split(col("value"), "\\s+").getItem(1).cast(LongType).alias("dst")
      )
      .filter(col("src").isNotNull && col("dst").isNotNull)
    
    // Convert to undirected
    val edgesRev = edgesDF.select(
      col("dst").alias("src"),
      col("src").alias("dst")
    )
    
    val graph = edgesDF.union(edgesRev).distinct().cache()
    
    println(s"Initial edges: ${graph.count()}")
    
    // Run CCF
    val startTime = System.currentTimeMillis()
    val (finalGraph, iterations) = CCF_DataFrame.runCCF(graph, spark)
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    // Count components
    val components = finalGraph.groupBy("dst").count().count()
    
    println(s"\nResults:")
    println(s"  Total components: $components")
    println(s"  Iterations: $iterations")
    println(f"  Total time: $totalTime%.2f seconds")
    println(f"  Avg time per iteration: ${totalTime/iterations}%.2f seconds")
  }
  
  /**
   * Run DataFrame on synthetic graph
   */
  def runSyntheticDataFrame(spark: SparkSession): Unit = {
    println("Generating synthetic graph (10,000 nodes, avg degree 10)...")
    val graph = CCF_DataFrame.generateSyntheticGraph(spark, 10000, 10).cache()
    val edgeCount = graph.count()
    println(s"Generated: $edgeCount edges")
    
    val startTime = System.currentTimeMillis()
    val (_, iterations) = CCF_DataFrame.runCCF(graph, spark)
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    println(f"\nCompleted in $iterations iterations, ${totalTime}%.2f seconds")
  }
  
  /**
   * Run full experimental analysis
   */
  def runExperimentalAnalysis(spark: SparkSession, sc: org.apache.spark.SparkContext): Unit = {
    import spark.implicits._
    
    println("\nStarting comprehensive experimental analysis...")
    println("This will test both RDD and DataFrame implementations")
    println("on graphs of increasing size.\n")
    
    val graphSizes = Array(1000L, 5000L, 10000L, 50000L, 100000L)
    val avgDegree = 10
    val results = scala.collection.mutable.ArrayBuffer[CCF_Experimental_Analysis.ExperimentResult]()
    
    println(s"Graph sizes to test: ${graphSizes.mkString(", ")} nodes")
    println(s"Average degree: $avgDegree\n")
    
    for (size <- graphSizes) {
      println("\n" + "="*80)
      println(s"Testing graph with $size nodes (avg degree: $avgDegree)")
      println("="*80)
      
      // Generate graphs
      println("\nGenerating synthetic graph...")
      val (graphRDD, graphDF) = CCF_Experimental_Analysis.generateGraphs(spark, sc, size, avgDegree)
      
      val edgeCount = graphRDD.count()
      println(s"Generated graph with $edgeCount edges")
      
      // Test RDD
      println("\n--- Testing RDD Implementation ---")
      val rddResult = CCF_Experimental_Analysis.runRDDExperiment(graphRDD, sc, size, edgeCount)
      results += rddResult
      
      // Test DataFrame
      println("\n--- Testing DataFrame Implementation ---")
      val dfResult = CCF_Experimental_Analysis.runDataFrameExperiment(graphDF, spark, size, edgeCount)
      results += dfResult
      
      // Print comparison
      CCF_Experimental_Analysis.printComparison(rddResult, dfResult)
      
      // Cleanup
      graphRDD.unpersist()
      graphDF.unpersist()
    }
    
    // Print overall summary
    CCF_Experimental_Analysis.printOverallSummary(results.toSeq)
    
    // Save results
    CCF_Experimental_Analysis.saveResultsToCSV(results.toSeq, "results/csvs/ccf_experiment_results_scala.csv")
    
    println("\n" + "="*80)
    println("Experimental analysis complete!")
    println("Results saved to: results/csvs/ccf_experiment_results_scala.csv")
    println("="*80)
  }
  
  /**
   * Run complete analysis suite
   */
  def runCompleteAnalysis(args: Array[String], spark: SparkSession, sc: org.apache.spark.SparkContext): Unit = {
    // 1. Run experimental analysis
    println("\n" + "="*80)
    println("PHASE 1: EXPERIMENTAL ANALYSIS (RDD vs DataFrame)")
    println("="*80)
    runExperimentalAnalysis(spark, sc)
    
    // 2. If input file provided, run both implementations on it
    if (args.length > 0 && args(0).nonEmpty) {
      println("\n" + "="*80)
      println("PHASE 2: TESTING ON PROVIDED GRAPH FILE")
      println("="*80)
      
      println("\n--- Running RDD Implementation ---")
      runRDDImplementation(args, sc)
      
      println("\n--- Running DataFrame Implementation ---")
      runDataFrameImplementation(args, spark)
    }
    
    println("\n" + "="*80)
    println("COMPLETE ANALYSIS FINISHED")
    println("="*80)
    println("\nGenerated files:")
    println("  - results/csvs/ccf_experiment_results_scala.csv: Experimental analysis results")
    println("\nCheck the console output above for detailed results.")
  }
  
  /**
   * Print usage information
   */
  def printUsage(): Unit = {
    println("""
      |Usage: spark-submit --class CCF_Main [options] <mode> [input-file]
      |
      |Modes:
      |  rdd          Run RDD implementation on input file
      |  dataframe    Run DataFrame implementation on input file
      |  experiment   Run experimental analysis (RDD vs DataFrame comparison)
      |  all          Run complete analysis suite (default)
      |
      |Examples:
      |  spark-submit --class CCF_Main --master local[*] ccf.jar all
      |  spark-submit --class CCF_Main --master local[*] ccf.jar rdd web-Google.txt
      |  spark-submit --class CCF_Main --master local[*] ccf.jar experiment
      |
      """.stripMargin)
  }
}
