/**
 * Illustrates flatMap + countByValue for wordcount.
 */


import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    val wordStartsWithh = words.filter(word => word.startsWith("h"))

    val addToCounts = (n: Int, v: Int) => n+1
    val sum = (p1 :Int, p2: Int) => p1 + p2
    val countsWordsWithh = wordStartsWithh.map(word => (word, 1)).repartition(1).aggregateByKey(0)(addToCounts, sum).sortBy(_._2, false)
    val countOfRecords = countsWordsWithh.count();
    println("Count of words that start with h: " + countOfRecords)
    // Save the word count back out to a text file, causing evaluation.
    countsWordsWithh.saveAsTextFile("output")
  }
}

