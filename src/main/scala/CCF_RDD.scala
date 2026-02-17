import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
 * Connected Components Finder (CCF) Algorithm - RDD Implementation in Scala
 * Based on the paper: https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf
 * 
 * The CCF algorithm finds connected components in a graph using iterative MapReduce operations.
 * Each iteration propagates the minimum node ID through the graph until convergence.
 */
object CCF_RDD {
  
  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf()
      .setAppName("CCF_WebGoogle_RDD")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    try {
      // Read and process input file
      val inputPath = if (args.length > 0) args(0) else "web-Google.txt"
      println(s"Reading graph from: $inputPath")
      
      // Load edges from file
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
      
      // Convert to undirected graph by adding reverse edges
      val reverseEdges = rawEdges.map { case (src, dst) => (dst, src) }
      var graph = rawEdges.union(reverseEdges).distinct().cache()
      
      println(s"Initial edges: ${graph.count()}")
      
      // Run CCF algorithm
      val (finalGraph, iterations) = runCCF(graph, sc)
      
      // Count components
      val components = finalGraph.groupByKey().count()
      println(s"\nTotal components: $components")
      println(s"Finished in $iterations iterations")
      
    } finally {
      sc.stop()
    }
  }
  
  /**
   * Run the CCF algorithm on the graph
   * 
   * @param initialGraph Initial edge RDD
   * @param sc SparkContext
   * @return Tuple of (final graph, number of iterations)
   */
  def runCCF(initialGraph: RDD[(Long, Long)], sc: SparkContext): (RDD[(Long, Long)], Int) = {
    var graph = initialGraph
    var iteration = 0
    var changed = true
    
    while (changed) {
      val startTime = System.currentTimeMillis()
      iteration += 1
      println(s"\nIteration $iteration")
      
      // Group neighbors by node
      val grouped = graph.groupByKey()
      
      // Propagate minimum IDs
      val updated = grouped.flatMap { case (node, neighbors) =>
        val neighborList = neighbors.toList
        
        if (neighborList.isEmpty) {
          Seq.empty[(Long, Long)]
        } else {
          val minId = (node :: neighborList).min
          
          if (minId < node) {
            val results = ArrayBuffer[(Long, Long)]()
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
      
      // Efficient change detection
      val diff = updated.subtract(graph)
      changed = !diff.isEmpty()
      
      // Replace old graph
      graph.unpersist()
      graph = updated
      
      val elapsedTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"Iteration time: $elapsedTime%.2f seconds")
    }
    
    (graph, iteration)
  }
  
  /**
   * Generate a synthetic graph for testing
   * 
   * @param sc SparkContext
   * @param numNodes Number of nodes
   * @param avgDegree Average degree per node
   * @return RDD of edges
   */
  def generateSyntheticGraph(sc: SparkContext, numNodes: Long, avgDegree: Int): RDD[(Long, Long)] = {
    val numEdges = (numNodes * avgDegree / 2).toLong
    
    val edges = sc.parallelize(0L until numEdges).map { _ =>
      val src = scala.util.Random.nextLong().abs % numNodes
      val dst = scala.util.Random.nextLong().abs % numNodes
      if (src != dst) (src, dst) else (src, (dst + 1) % numNodes)
    }.distinct()
    
    // Make undirected
    val reverseEdges = edges.map { case (src, dst) => (dst, src) }
    edges.union(reverseEdges).distinct()
  }
}
