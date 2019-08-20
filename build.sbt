name := "spark-sparse-vector"

version := "0.1"

scalaVersion := "2.12.9"


val sparkVersion = "2.4.3"

scalacOptions := Seq("-unchecked", "-deprecation")

// remove version-specific scala dirs
crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion %"test" classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalactic" %% "scalactic" % "3.0.8" % Test
)
