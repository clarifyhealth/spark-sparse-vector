name := "spark-sparse-vector"
organization := "clarifyhealth"
version := "0.2"

scalaVersion := "2.12.8"

val sparkVersion = "3.1.1"

scalacOptions := Seq("-unchecked", "-deprecation")

// turn off parallel tests
parallelExecution in Test := false
// remove version-specific scala dirs
crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,  
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion %"test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion %"test" classifier "tests",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "org.scalactic" %% "scalactic" % "3.1.0" % Test
)
