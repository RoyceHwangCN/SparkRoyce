/**
  * Created by F7686297 on 2017/2/15.
  */
package importtest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode

object importtest {
  case class DataFormat
  (
    name : String,
    age  : String
  )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val url = "jdbc:mysql://10.146.67.230:3306/test"
    val prop = new java.util.Properties
    prop.setProperty("user", "Royce")
    prop.setProperty("password", "Royce")

    val data = sc.textFile(args(0))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val dataframe = data.map(_.split(",")).map(p => DataFormat(p(0),p(1))).toDF()

    dataframe.write.mode(SaveMode.Append).jdbc(url,"test",prop)

    sc.stop()
  }

}
