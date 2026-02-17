name := "CCF-Spark-Scala"

version := "1.0"

scalaVersion := "2.13.12"

// Set default main class to the unified runner
Compile / mainClass := Some("CCF_Main")

// Spark dependencies - updated to Spark 4.x which requires Scala 2.13
val sparkVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

// Java options to avoid reflection warnings
run / fork := true
run / javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx4G",
  "-XX:+UseG1GC"
)

// Assembly settings for creating fat JAR
assembly / assemblyMergeStrategy := {
  case x if x.endsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case x if x.endsWith("META-INF/LICENSE") => MergeStrategy.discard
  case x if x.endsWith("META-INF/NOTICE") => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assembly / assemblyJarName := "ccf-spark-scala.jar"

// Exclude Spark dependencies from assembly JAR since they're provided
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter(_.data.getName.contains("spark-"))
}
