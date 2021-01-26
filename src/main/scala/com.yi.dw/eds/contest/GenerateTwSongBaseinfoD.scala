package com.yi.dw.eds.contest

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yi.dw.common.{ConfigUtils, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


object GenerateTwSongBaseinfoD {
  val localRun = ConfigUtils.LOCAL_RUN
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  val hiveMetastoreUri = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDatabase = ConfigUtils.HIVE_DATABASE


  /**
    * 生成TW层 TW_SONG_BASEINFO_D 数据表
    * 主要是读取Hive中的ODS层 TO_SONG_INFO_D 表生成 TW层 TW_SONG_BASEINFO_D表，
    *
    */
//获取专辑的名称
  val getAlbumName: String => String = (albumInfo: String) => {
    var albumName = ""
    try {
      val jsonArray: JSONArray = JSON.parseArray(albumInfo)
      albumName = jsonArray.getJSONObject(0).getString("name")
    } catch {
      case e: Exception => {
        if (albumInfo.contains("《") && albumInfo.contains("》")) {
          albumName = albumInfo.substring(albumInfo.indexOf('《'),albumInfo.indexOf('》') + 1)
        } else {
          albumName = "无此专辑"
        }
      }

    }
    albumName

  }

  //获取发行时间
  val getPostTime:String=>String=(postTime:String)=>{
    DateUtils.formateDate(postTime)
  }

  //获取歌手信息
  /**
    * (1)方法的输入和输出
    * (String,String,String)=>String
    * (2)"="是赋值
    * (3)参数和参数的类型,=>这后面是一个匿名内部类
    * (singerInfos:String,singer:String,nameOrId:String)=>{}
    */
  val getSingerInfo:(String,String,String)=>String=(singerInfos:String,singer:String,nameOrId:String)=>{
    var singerNameOrSingerId=""
    try {
      val jsonArray: JSONArray = JSON.parseArray(singerInfos)
      if ("singer1".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("name")
      }else if ("singer1".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("id")
      }else if ("singer2".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("name")
      }else if ("singer2".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("id")
      }
    } catch {
      case e:Exception =>{
        singerNameOrSingerId
      }
    }
    singerNameOrSingerId

  }
  //获取授权公司
  //SELECT authorized_company  FROM  dw.song  where LENGTH (authorized_company)>0
  val getAuthCompany:String=>String=(authCompanyInfo:String)=>{
    var authCompanyName="乐心曲库"
    try {
      val jsonObject: JSONObject = JSON.parseObject(authCompanyInfo)
      authCompanyName = jsonObject.getString("name")
    } catch {
      case e:Exception =>{
        authCompanyName
      }
    }
    authCompanyName


  }

  //获取产品类型
  //[8.0,2.0,9]
  val getPrdctType:(String=>ListBuffer[Int])=(productTypeInfo :String)=>{
    val productType=new ListBuffer[Int]
    if(!"".equals(productTypeInfo)){
    val productList=productTypeInfo.stripPrefix("[").stripSuffix("]").split(",")
      productList.foreach(p=>{
        //转成小数在转成整数
        productType.append(p.toDouble.toInt)
      })
    }
    productType
  }



  def main(args: Array[String]): Unit = {

/*    println(getSingerInfo("[{\"name\":\"谢和弦\",\"id\":\"15312\"},{\"name\":\"蓝心湄\",\"id\":\"15575\"}]","singer1","name"))
    println(getSingerInfo("[{\"name\":\"谢和弦\",\"id\":\"15312\"},{\"name\":\"蓝心湄\",\"id\":\"15575\"}]","singer1","id"))
    println(getSingerInfo("[{\"name\":\"谢和弦\",\"id\":\"15312\"},{\"name\":\"蓝心湄\",\"id\":\"15575\"}]","singer2","name"))
    println(getSingerInfo("[{\"name\":\"谢和弦\",\"id\":\"15312\"},{\"name\":\"蓝心湄\",\"id\":\"15575\"}]","singer2","id"))*/

if (localRun) {
      sparkSession = SparkSession.builder().appName("localRun").master("local")
        .config("hive.metastore.uris", hiveMetastoreUri).enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    } else {
      sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    }


    import org.apache.spark.sql.functions._ //导入函数，可以使用 udf、col 方法
    //构建转换数据的udf
    val udfGetAlbumName = udf(getAlbumName)
    val udfGetPostTime = udf(getPostTime)
    val udfGetSingerInfo = udf(getSingerInfo)
    val udfGetAuthCompany = udf(getAuthCompany)
    val udfGetPrdctType = udf(getPrdctType)
//追加字段，创建临时表
    sparkSession.sql(s"use $hiveDatabase")
    sparkSession.table("TO_SONG_INFO_D")
      .withColumn("ALBUM",udfGetAlbumName(col("ALBUM")))
      .withColumn("POST_TIME",udfGetPostTime(col("POST_TIME")))
      //col-是已经存在的一列变量字段，lit-是追加一列常量字段
      .withColumn("SINGER1",udfGetSingerInfo(col("SINGER_INFO"),lit("singer1"),lit("name")))
      .withColumn("SINGER1ID",udfGetSingerInfo(col("SINGER_INFO"),lit("singer1"),lit("id")))
      .withColumn("SINGER2",udfGetSingerInfo(col("SINGER_INFO"),lit("singer2"),lit("name")))
      .withColumn("SINGER2ID",udfGetSingerInfo(col("SINGER_INFO"),lit("singer2"),lit("id")))
      .withColumn("AUTH_CO",udfGetAuthCompany(col("AUTH_CO")))
      .withColumn("PRDCT_TYPE",udfGetPrdctType(col("PRDCT_TYPE")))
      .createTempView("TEMP_TO_SONG_INFO_D")

    /**
      * 清洗数据，将结果保存到 Hive TW_SONG_BASEINFO_D 表中
      */
    sparkSession.sql(
      s"""
         |select NBR,
         |       nvl(NAME,OTHER_NAME) as NAME,
         |       SOURCE,
         |       ALBUM,
         |       PRDCT,
         |       LANG,
         |       VIDEO_FORMAT,
         |       DUR,
         |       SINGER1,
         |       SINGER2,
         |       SINGER1ID,
         |       SINGER2ID,
         |       0 as MAC_TIME,
         |       POST_TIME,
         |       PINYIN_FST,
         |       PINYIN,
         |       SING_TYPE,
         |       ORI_SINGER,
         |       LYRICIST,
         |       COMPOSER,
         |       BPM_VAL,
         |       STAR_LEVEL,
         |       VIDEO_QLTY,
         |       VIDEO_MK,
         |       VIDEO_FTUR,
         |       LYRIC_FTUR,
         |       IMG_QLTY,
         |       SUBTITLES_TYPE,
         |       AUDIO_FMT,
         |       ORI_SOUND_QLTY,
         |       ORI_TRK,
         |       ORI_TRK_VOL,
         |       ACC_VER,
         |       ACC_QLTY,
         |       ACC_TRK_VOL,
         |       ACC_TRK,
         |       WIDTH,
         |       HEIGHT,
         |       VIDEO_RSVL,
         |       SONG_VER,
         |       AUTH_CO,
         |       STATE,
         |       case when size(PRDCT_TYPE) =0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
         |    from TEMP_TO_SONG_INFO_D
         |    where NBR != ''
       """.stripMargin).write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TW_SONG_BASEINFO_D")

    println("========all finished=======")


  }

}
