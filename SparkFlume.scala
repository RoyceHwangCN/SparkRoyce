package dcslog


import java.net.InetSocketAddress
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.streaming.flume.FlumeReceiver



object dcslog
{
  // 定义case class,在这里我们需要提取主机名,记录时间以及信息写入SequoiaDB
  case class LogFormat
  (
    hostname : String,
    location : String,
    logtime : Timestamp,
    information : String
  )

  // 主函数,提供接收命令行输入的功能
  def main(args: Array[String]): Unit = {

    if (args.length != 2)
    {
      System.err.println("<Usage: hostname port!>")
      System.exit(1)
    }

  // 定义getTimeStamp函数,将字符串转化为时间戳
    def getTimeStamp(x:Any): java.sql.Timestamp =
    {
      // 确定字符串的日期格式,如果传入的参数格式不对,程序会报错
      val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
      if(x.toString() == null)
        return null
      else
      {
        val date = format.parse(x.toString())
        val time = new Timestamp(date.getTime())
        return time
      }
    }

    /**def geteventsheader(x:SparkFlumeEvent): String =
    {
      val header = new String(x.event.getHeaders().values().toString())
      return header
    }

    def geteventsbody(x:SparkFlumeEvent): String =
    {
      val body = new String(x.event.getBody().array())
      return body
    }*/

    def getEventsInfo(x:SparkFlumeEvent) :Array[String] =
    {
      val header = new String(x.event.getHeaders().values().toString())
      val body = new String(x.event.getBody().array())
      val header_sub = header.split("-")
      val body_sub = body.split(";")

      return Array(header_sub(1),header_sub(2),body_sub(0),body_sub(1))
    }

    /*def getInfoOfEvents(x:SparkFlumeEvent)
    {
      val header = new String(x.event.getHeaders().values().toString())
      val body = new String(x.event.getBody().array())
      val header_sub = header.split("-")
      val body_sub = body.split(";")

      return dcslog(header_sub(1),header_sub(2),getTimeStamp(body_sub(0)),body_sub(1))
    }*/



    /**def AdditionOne(x:Int): Unit =
    {
      if(x < 0)
        return null
      else
        {
          return x+1
        }
    }*/

    // 初始化SparkConf
    val conf = new SparkConf().setAppName("dcslog").setMaster("spark://10.146.65.241:7077")
      .setJars(List("/data/dcslog.jar",
      "/soft/spark-streaming-flume_2.10-1.6.1.jar",
      "/soft/apache-flume-1.7.0-bin/lib/flume-ng-core-1.7.0.jar",
      "/soft/apache-flume-1.7.0-bin/lib/flume-ng-sdk-1.7.0.jar",
      "/soft/apache-flume-1.7.0-bin/lib/spark-streaming-flume-sink_2.10-1.6.1.jar"))

    // 初始化SparkContext
    val sc = new SparkContext(conf)

    // 初始化SparkStreaming,流的间隔为10s
    val ssc = new StreamingContext(sc,Seconds(1))

    // 初始化Flume数据流,该对象接收从Flume获得的数据,它的本质是event流
    val FlumeStream = FlumeUtils.createStream(ssc,args(0),Integer.parseInt(args(1)))

    // 初始化hiveContext,以供我们将数据写入与Sequoiadb对应的hive表中
    // 此处用sqlContext也可?
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._


    // 对于每个流对象都做处理
    FlumeStream.foreachRDD(
      rdd => {

        //var FinalDF = Seq(dcslog("","",getTimeStamp("1900-01-01 00:00:00"),"")).toDF()
        // 将每个rdd的值存起来
        // Action 1: 此处rdd为任何名称均可,这是Spark的特性
        // Acrion 2: 若此处不加collect,直接用event = rdd 会报错,原因待究

        val events = rdd.collect()

        //val eventsRDD : org.apache.spark.rdd.RDD[SparkFlumeEvent] = sc.parallelize(events)

        //events.foreach(println)

        val tempRDD : org.apache.spark.rdd.RDD[Array[String]] = sc.parallelize(events.map(event => getEventsInfo(event)))

        //tempRDD.foreach(println)

        val DFOfEvents = tempRDD.map(p => (LogFormat(p(0),p(1),getTimeStamp(p(2)),p(3)))).toDF()

        DFOfEvents.show(10)

        DFOfEvents.registerTempTable("logTempTable")

        hiveContext.sql("use bbclog")
        hiveContext.sql("insert into dcslog select * from logTempTable")

        //events.map(event => {println(getEventsInfo(event))})

        //events.map(event => println(new String(event.event.getHeaders().values().toString())))

        //val headerRDD : org.apache.spark.rdd.RDD[String] = sc.parallelize(events.map(event => geteventsheader(event)))

        //val headerDF = headerRDD.map(_.split("-")).map(p => LogFormat(p(1),getTimeStamp()p(2))).toDF()

        //headerDF.show()


        /**headerRDD.map(e => println(e))
        headerRDD.collect()
        headerRDD.foreach(println)*/

        //val headerRDD_final : org.apache.spark.rdd.RDD[String] = sc.parallelize(headerRDD)

        //headerRDD_final.collect()
        //headerRDD_final.foreach(println)


        //events.map(event => println(new String(event.event.getBody().array())))

        //val bodyRDD : org.apache.spark.rdd.RDD[String] = sc.parallelize(events.map(event => geteventsbody(event)))

        //val bodyDF = bodyRDD.map(_.split(";")).map(p=> LogFormat("","",getTimeStamp(p(0)),p(1))).toDF()

        //bodyDF.show()

        //val bodyRDD_final : org.apache.spark.rdd.RDD[String] = sc.parallelize(bodyRDD)

        //bodyRDD_final.collect()
        //bodyRDD_final.foreach(println)

        /**bodyRDD.map(e => println(e))
        bodyRDD.collect()
        bodyRDD.foreach(println)*/


        // 对于事件流中的每一个事件做处理



        /**for (event <- events) {
          // 调用SparkFlumeEvent下层的AvroFlumeEvent的get函数获取event的head和body值
          // 在Flume中,文本都是通过char流的方式处理,因此此处转化为array[char]即String
          // Header是map(key,values)格式的,因此需要把value筛选出来再转化为字符串
          //val header = new String(event.event.getHeaders().values().toString())
          val header = geteventsheader(event)
          //val body = new String(event.event.getBody().array())
          val body = geteventsbody(event)

          print(header)
          print(body)
        }*/

          /**val header_sub = header.split("-")
          //println(header)
          //println(header_sub(1))
          //println(header_sub(2))
          val body_sub = body.split(";")

          val logDF = Seq(LogFormat(header_sub(1), header_sub(2), getTimeStamp(body_sub(0)), body_sub(1))).toDF()
          //logDF.show()

          logDF.registerTempTable("logTempTable")

          //FinalDF = FinalDF.unionAll(logDF)
          hiveContext.sql("use bbclog")
          hiveContext.sql("insert into dcslog select * from logTempTable")8?


        }*/

        //FinalDF.show()
        //FinalDF.collectAsList().show()
        /**FinalDF.registerTempTable("logTempTable")
        hiveContext.sql("use bbclog")
        hiveContext.sql("insert into dcslog select * from logTempTable where hostname != ''")*/

      }
    )



    ssc.start()
    ssc.awaitTermination()
  }
}