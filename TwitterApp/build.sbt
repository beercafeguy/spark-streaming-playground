name := "TwitterApp"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
//libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
  //"org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"
//)

//resolvers += "Maven Central" at "http://repo1.maven.org/maven2/"
resolvers += "Maven" at "http://central.maven.org/maven2/"

//I am using spark/lib for compiling as SBT has some issue in my machine

//Once a context has been started, no new streaming computations can be set up or added to it.
//Once a context has been stopped, it cannot be restarted.
//Only one StreamingContext can be active in a JVM at the same time.
//stop() on StreamingContext also stops the SparkContext.
// To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
// A SparkContext can be re-used to create multiple StreamingContexts,
// as long as the previous StreamingContext is stopped (without stopping the SparkContext)
// before the next StreamingContext is created.