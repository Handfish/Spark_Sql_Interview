## Spark-Scala Interview Application

Built with sbt

`sbt assembly to build

`spark-submit --class com.kennyudovic.sparkScalaInterview.tableDifferences target/scala-2.11/interview-proj-assembly-0.1.jar 1.csv 2.csv` to run via shell.

`spark-submit --class com.kennyudovic.sparkScalaInterview.tableDifferencesSQL target/scala-2.11/interview-proj-assembly-0.1.jar 1.csv 2.csv` to run Spark SQL Queries via shell.

