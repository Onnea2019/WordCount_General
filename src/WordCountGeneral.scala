import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountGeneral {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkWordCount_General")
    val sc = new SparkContext(conf)

    val inputFile = args(1)
    val lines: RDD[String] = sc.textFile(inputFile, 1)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val count = pairs.reduceByKey( _ + _ )

    val outputFile = args(2)
    count.saveAsTextFile(outputFile)

    sc.stop()
  }
}

//1：读hdfs的文件，写入hdfs目录，路径都是hdfs的
/* ./spark-submit --master spark://192.168.83.197:7077
--class WordCountGeneral /root/Spark_Project_jar/WordCountGeneral.jar
--files /usr/local/hdfs_tmp/word.txt /usr/local/hdfs_out/WordCountGeneral_out */

/* hadoop fs -cat /usr/local/hdfs_out/WordCountGeneral_out/part-00000 */

//2：local(未在集群中运行，读写都在local里)
/* ./spark-submit --class WordCountGeneral /root/Spark_Project_jar/WordCountGeneral.jar
--files file:///root/Spark_Project_jar/word.txt file:///root/Spark_Project_jar/WordCountGeneral_out */
//生成的WordCountGeneral_out是个目录，里面包含part-00000
/* cat part-00000 */

//3：提交到集群中运行，需要提前保证每个机器里在相同目录下都有word.txt，否则报错，)
/* ./spark-submit --master spark://master:7077
--class WordCountGeneral /root/Spark_Project_jar/WordCountGeneral.jar
--files file:///root/Spark_Project_jar/word.txt file:///root/Spark_Project_jar/WordCountGeneral_master_out */
/**
结果：master相应目录出现，但是里面只包含_SUCCESS, slave1没有相关目录
 slave2出现相关目录，cd进入，part-00000在_temporary/0/task_20180531101750_0003_m_000000/内
（结果会出现在执行计算的机器中，随机，不会在master中）

WordCountGeneral_master_out/
└── _temporary
    └── 0
        ├── task_20180531101750_0003_m_000000
        │   └── part-00000
        └── _temporary

  **/
