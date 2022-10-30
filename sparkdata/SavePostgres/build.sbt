assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "SavePostgres"
  )
libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-streaming" % "3.0.1"),
  ("org.apache.spark" %% "spark-core" % "3.0.1"),
  ("org.apache.spark" %% "spark-sql" % "3.0.1"),
  ("org.apache.spark" %% "spark-hive" % "3.0.1" % "provided"),
  ("org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.0.1")
)

libraryDependencies += "org.postgresql" % "postgresql" % "42.3.5"
// https://mvnrepository.com/artifact/javax.mail/mail
libraryDependencies += "javax.mail" % "mail" % "1.5.0-b01"
libraryDependencies += "com.sun.mail" % "jakarta.mail" % "2.0.1"
