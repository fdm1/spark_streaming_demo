package org.apache.spark.programs 
object WordCount{ 
	def main(args:Array[String]) { 
	val conf = new SparkConf 
	conf.setAppName("WordCount") 
	val sc = new SparkContext(conf) 
	val input = sc.parallelize(Array("this,is,a,ball","it,is,a,cat","john,is,in,town,hall")) 
	val words = input.flatMap{record => record.split(",")} 
	val wordPairs = words.map(word => (word,1)) 
	val wordCounts = wordPairs.reduceByKey{(a,b) => a+b} 
	val result = wordCounts.collect 
	println("Displaying the WordCounts:") 
	result.foreach(println) }}
