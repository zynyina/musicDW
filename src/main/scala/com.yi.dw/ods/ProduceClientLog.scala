package com.yi.dw.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yi.dw.base.PairRDDMultipleTextOutputFormat
import com.yi.dw.common.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProduceClientLog {

  private val localRun = ConfigUtils.LOCAL_RUN;
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private var sparkSession: SparkSession = _
  private var sc: SparkContext = _
  private var clientLogInfos: RDD[String] = _
  private val hdfsClientLogPath: String = ConfigUtils.HDFS_CLIENT_LOG_PATH
  private var hiveDataBase:String=ConfigUtils.HIVE_DATABASE


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(s"需要指定一个日期")
      System.exit(1);
    }
    //日期格式:20201231
    val logDate = args(0)
    if (localRun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ProduceClientLog")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile("F:\\IdeaProjects\\yina\\data\\currentday_clientlog.tar.gz")
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(s"${hdfsClientLogPath}/currentday_clientlog.tar.gz")
    }

    //处理数据,组织K,V格式的数据
    val tableNameAndInfos6 = clientLogInfos.map(line => line.split("&"))
      .filter(f => f.length == 6)
      .map(line => (line(2), line(3)))
    //打印长度不为6的
    val tableNameAndInfosOther = clientLogInfos.map(line => line.split("&"))
      .filter(f => f.length != 6)
      .map(line => println(s"长度不为6的数据:${line}"))
    //转换数据，将数据分别以表名的方式存储在某个路径中
    tableNameAndInfos6.map(tp => {
      val tableName = tp._1
      val tableInfos = tp._2
      if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(tableName)) {
        val jsonObject: JSONObject = JSON.parseObject(tableInfos)
        val songid = jsonObject.getString("songid") //歌曲ID
        val mid = jsonObject.getString("mid") //机器ID
        val optrateType = jsonObject.getString("optrate_type") //0:点歌, 1:切歌,2:歌曲开始播放,3:歌曲播放完成,4:录音试听开始,5:录音试听切歌,6:录音试听完成
        val uid = jsonObject.getString("uid") //用户ID（无用户则为0）
        val consumeType = jsonObject.getString("consume_type")
        //消费类型：0免费；1付费
        val durTime = jsonObject.getString("dur_time") //总时长单位秒（operate_type:0时此值为0）
        val sessionId = jsonObject.getString("session_id") //局数ID
        val songName = jsonObject.getString("songname") //歌曲名
        val pkgId = jsonObject.getString("pkg_id")
        //套餐ID类型
        val orderId = jsonObject.getString("order_id") //订单号
        (tableName, songid + "\t" + mid + "\t" + optrateType + "\t" + uid + "\t" + consumeType + "\t" + durTime + "\t" + sessionId + "\t" + songName + "\t" + pkgId + "\t" + orderId)
      } else {
        tp
      }
    }).saveAsHadoopFile(//自定义保存格式，方便识别表名。没有自定义之前是part-0000....
      s"$hdfsClientLogPath/logdata/$logDate",
      classOf[String],
      classOf[String],
      classOf[PairRDDMultipleTextOutputFormat]
    )

    /**
      * 在Hive中创建 ODS层的 TO_CLIENT_SONG_PLAY_OPERATE_REQ_D 表
      */
    sparkSession.sql(s"use $hiveDataBase")
    sparkSession.sql(
      s"""
        |CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
        |`SONGID` string,  --歌曲ID
        | `MID` string,     --机器ID
        | `OPTRATE_TYPE` string,  --操作类型
        | `UID` string,     --用户ID
        | `CONSUME_TYPE` string,  --消费类型
        | `DUR_TIME` string,      --时长
        | `SESSION_ID` string,    --sessionID
        | `SONGNAME` string,      --歌曲名称
        | `PKG_ID` string,        --套餐ID
        | `ORDER_ID` string       --订单ID
        |)
        |partitioned by (data_dt string)
        |row format delimited fields TERMINATED by "\t"
        |LOCATION 'hdfs://hadoop000:8020/user/hive/warehouse/data/song/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D'
      """.stripMargin)

    sparkSession.sql(
      s"""
        |
        |load data inpath
        |'$hdfsClientLogPath/logdata/$logDate/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
        |into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt=$logDate)
        |
      """.stripMargin)


  }

}
