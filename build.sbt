name := "sbtExampleProject"

version := "0.1"

scalaVersion := "2.12.13"

idePackagePrefix := Some("com.newday.example")

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
  Resolver.sonatypeRepo("public")
)