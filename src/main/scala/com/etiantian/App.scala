package com.etiantian

import java.io.{File, FileInputStream}
import java.time.LocalDate
import java.util.Properties

import com.etiantian.udf.FindChangeDate
import org.apache.log4j.LogManager
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import org.elasticsearch.spark.sql._

import scala.util.Try

/**
 * Hello world!
 *
 */
object App {
  val logger = LogManager.getLogger("ResourceShare")

  def main(args: Array[String]): Unit = {
    val configFile = new File(args(0))
    if (!configFile.exists()) {
      logger.error("Missing config.properties file!")
    }

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))      //加载配置文件

    val sparkConf = new SparkConf().setAppName("ycf:ResourceShare")
      .set("es.index.auto.create", "true")
      .set("es.write.operation", "upsert")
      .set("es.nodes",properties.getProperty("es.nodes"))
      .set("es.port",properties.getProperty("es.port"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val beginDate = if (args.length>=2) args(1) else LocalDate.now().plusDays(-1).toString
    val endDate = if (args.length>=3) args(2) else LocalDate.now().toString

    println("beginDate = "+ beginDate + " and endDate = "+endDate)

    import sqlContext.implicits._

    def isUpSertRes(url:String) = {
      url.contains("resource?m=doUpdateColumn") ||
        url.contains("resource?m=changeLibMangeStatus") ||
        url.contains("resource?m=doLibAdd") ||
        url.contains("tpres?doNewUploadResource") ||
        url.contains("tpres?m=doNewUpdResource")
    }

    def getShareStatus(jsonStr:String) = {
      var shareStaus = -1
      var json = new JSONObject()
      if(Try(new JSONObject(jsonStr.toLowerCase)).isSuccess) {
        json =new JSONObject(jsonStr.toLowerCase)
      }
      if (json.has("sharestatus"))
        shareStaus = json.getInt("sharestatus")
      shareStaus
    }

    def getResId(jsonStr:String) = {
      var json = new JSONObject()
      if(Try(new JSONObject(jsonStr.toLowerCase)).isSuccess) {
        json =new JSONObject(jsonStr.toLowerCase)
      }
      if (json.has("resid") && Try(json.getLong("resid")).isSuccess) {
        json.getLong("resid")
      }
      else
        0l
    }


    def getUserId(jsonStr:String) = {
      var json = new JSONObject()
      if(Try(new JSONObject(jsonStr.toLowerCase)).isSuccess) {
        json =new JSONObject(jsonStr.toLowerCase)
      }
      if (json.has("userid") && Try(json.getLong("userid")).isSuccess) {
        json.getLong("userid")
      }
      else
        0l
    }

    sqlContext.udf.register("isUpSertRes", isUpSertRes _)
    sqlContext.udf.register("getShareStatus", getShareStatus _)
    sqlContext.udf.register("getResId", getResId _)
    sqlContext.udf.register("getUserId", getUserId _)

    val findChangeDate = new FindChangeDate()
    sqlContext.udf.register("findChangeDate", findChangeDate)

    val actionLogsDf = sqlContext.sql(s"select jid, getUserId(param_json) user_id_json, getResId(param_json) res_id, getShareStatus(param_json) share_status, c_time from sxlogsdb_action_logs where c_date < '$endDate' and c_date>='$beginDate' and isUpSertRes(url)").filter(
      "res_id != 0 and share_status != -1 and (jid is not null or user_id_json != 0)"
    ).cache()

    val userInfoDf = sqlContext.sql("select user_id, ett_user_id jid, dc_school_id from user_info_mysql")



    val df = actionLogsDf.join(userInfoDf, Seq("jid"), "left").selectExpr(
      "if(user_id_json=0, null, user_id_json) user_id_json",
      "user_id",
      "res_id",
      "share_status",
      "c_time"
    ).selectExpr(
      "coalesce(user_id_json, user_id) user_id",
      "res_id",
      "share_status",
      "c_time"
    )

    val userResShareDf = df.groupBy(
      "user_id",
      "res_id"
    ).agg(findChangeDate(df("share_status"),df("c_time"))).select(
      $"user_id",
      $"res_id",
      $"FindChangeDate(share_status,c_time)" as "c_date"
    ).filter("c_date is not null").join(userInfoDf, "user_id").selectExpr(
      "concat(dc_school_id,',',user_id,',',res_id,',',c_date) id",
      "user_id",
      "dc_school_id",
      "res_id",
      "1 is_share",
      "c_date"
    )


//    userResShareDf.saveToEs("user_resource_stat_w/user_resource_stat", Map(
//      "es.mapping.id"->"id",
//      "es.write.operation"->"upsert"
//    ))

    val resourceInfoDf = sqlContext.sql("select res_id, explode(split(concat(grade_id_junior,'|' ,grade_id_primary), '\\\\|')) grade_id, coalesce(subject_id,subject) subject_id from rs_resource_info_es")

    userResShareDf.join(resourceInfoDf, "res_id").selectExpr(
      "concat(id,',',grade_id, ',', subject_id) id",
      "user_id",
      "dc_school_id",
      "res_id",
      "is_share",
      "grade_id",
      "subject_id",
      "c_date"
    ).filter("grade_id is not null and length(grade_id)>0").saveToEs(
      "user_res_sub_grade_stat_w/user_res_sub_grade_stat", Map(
        "es.mapping.id"->"id",
        "es.write.operation"->"upsert"
      )
    )
  }
}
