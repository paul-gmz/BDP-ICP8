import org.apache.spark._

object SecondarySorting {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Installations\\Hadoop")
    val conf = new SparkConf().setAppName("SecondarySorting").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("input.txt")
    val pairs = input.map(line => line.split(",")).map{key => ((key(0) + "_" + key(1)), key(3).toInt)}
    val pairsGroup = pairs.groupByKey()
    val sorted = pairsGroup.map(x => (x._1, x._2.toList.sortBy(x => x)(Ordering[Int].reverse)))
    sorted.saveAsTextFile("output")
  }
}
