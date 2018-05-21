Please put the code in the Apache Spark source code folder, i put it in:
sql/hive/src/test/scala/org/apache/spark/sql/sources/TestHive.scala

Simply run the main method in this class.

If you need to run it outside of Apacha Spark package, you will need to modify Spark code, make 
Session class public to be called from outside.

