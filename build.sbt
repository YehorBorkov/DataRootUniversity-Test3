name := "DataRootUniversity-Test3"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.2.1",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.1",
  "org.postgresql" % "postgresql" % "42.1.4"
)
