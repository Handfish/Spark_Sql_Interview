## Inventiv Health: Spark-Scala and SQL Interview Applications

This program was part of my interview at Inventiv Health in Somerset, NJ. 
Both the Scala-Spark program and the bash script accomplish the same task. 

The goal of the program was to measure the changes between two tables of the same schema. Under the assumptions that
the first column is the primary key, the programs take two input csv tables and output a new table with an appended column of flag. Flag has four statuses:
 * Added
 * Deleted
 * Updated
 * No Change

Spark Scala Program Located in scalaSpark

Mysql Program located in sql

testdata in testcsvs
