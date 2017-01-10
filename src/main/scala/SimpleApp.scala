/**** Counting the number of Comedy movies and Action movies ****/
/*********************** at 2017.01.05 **************************/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "file:///home/hadoop/Caixinqi/ml-1m/movies.dat"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numCo = logData.filter(line => line.contains("Comedy")).count()
    val numAc = logData.filter(line => line.contains("Action")).count()
    println("Num of Comedy: %s, Num of Action: %s".format(numCo, numAc))
  }
}
