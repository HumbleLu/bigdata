import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem

object MultiInput {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Multiple Input Example")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val inPath1 = new org.apache.hadoop.fs.Path(args(0))
    val inPath2 = new org.apache.hadoop.fs.Path(args(1))
    val inPath3 = new org.apache.hadoop.fs.Path(args(2))
    val outPath = new org.apache.hadoop.fs.Path(args(3))

    if(fs.exists(outPath)){
        fs.delete(outPath)
    }

    val sc = new SparkContext(conf)
    val file1 = sc.textFile(inPath1.toString)
    var rdd1 = file1.map(line => (line.split(";")(1), line.split(";")(2)))

    val file2 = sc.textFile(inPath2.toString)
    var rdd2 = file2.filter(_.contains(",")).map(s => (s.split(",")(1).split(":")(1), s.split(",")(2).split(":")(1).replace("}", "")))

    val file3 = sc.textFile(inPath3.toString)
    var rdd3 = file3.map(s => (s.split(";")(1).split("=>")(1), s.split(";")(2).split("=>")(1).replace("]", "")))

    val rdd = rdd1.union(rdd2).union(rdd3)

    val res = rdd.combineByKey(
        (x: String) => (x.toInt, 1),
        (acc:(Int, Int), x) => (acc._1.toInt + x.toInt, acc._2.toInt + 1),
        (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1.toInt + acc2._1.toInt, acc1._2.toInt + acc2._2.toInt)).map(k => (k._1, k._2._1.toDouble / k._2._2.toDouble))

    res.saveAsTextFile(outPath.toString)
    }
}

