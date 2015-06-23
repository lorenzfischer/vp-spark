import AssemblyKeys._ // put this at the top of the file,leave the next line blank

assemblySettings

//crossBuildingSettings
// I'm following this here: http://prabstechblog.blogspot.ch/2014/04/creating-single-jar-for-spark-project.html

name := "vp-spark"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.4"

val sparkVersion = "1.3.1"

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
//  ("org.apache.spark" %% "spark-core" % "1.2.0").
//    //exclude("org.eclipse.jetty.orbit", "javax.servlet").
//    exclude("org.eclipse.jetty.orbit", "javax.transaction").
//    exclude("org.eclipse.jetty.orbit", "javax.mail").
//    exclude("org.eclipse.jetty.orbit", "javax.activation").
//    exclude("commons-beanutils", "commons-beanutils-core").
//    exclude("commons-collections", "commons-collections").
//    exclude("commons-logging", "commons-logging")
   "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  ,"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" // I need this in the DataMuncher
  ,("org.apache.hadoop" % "hadoop-streaming" % "1.0.4").
    exclude("org.apache.hadoop", "hadoop-common").
    exclude("org.apache.hadoop", "hadoop-core")
  ,"org.clapper" %% "argot" % "1.0.3"                 // the data muncher uses this option parser
  ,"com.github.scopt" %% "scopt" % "3.2.0"            // the LdaMuncher uses this option parser todo: there can only be one!
  ,"org.scalatest" %% "scalatest" % "2.2.1" % "test"  // used for testing
  ,"org.json4s" %% "json4s-native" % "3.2.11"         // I use this in the DataMuncher to parse json
  ,"org.json4s" %% "json4s-jackson" % "3.2.11"        // I use this in the DataMuncher to parse json
  ,"com.clearspring.analytics" % "stream" % "2.7.0"   // count-min-sketch for the IDF computation is in here
  ,"joda-time" % "joda-time" % "2.7"                  // used in the muncher
  ,"javax.servlet" % "javax.servlet-api" % "3.0.1"    // spark 1.3.0-SNAPSHOT needs this.. strange!!
)

resolvers ++= Seq(
   "Akka Repository" at "http://repo.akka.io/releases/"
  ,"Brian Clapper's Bintray" at "http://dl.bintray.com/bmc/maven"
  ,"Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

// otherwise my spark contexts go crazy
parallelExecution in Test := false