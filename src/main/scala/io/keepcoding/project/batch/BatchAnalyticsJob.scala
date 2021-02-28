package io.keepcoding.project.batch

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object BatchAnalyticsJob {
  def computeTotalBytesAgg(devicesDataDF: DataFrame, columnName: String, metricName: String, filterDate: OffsetDateTime): DataFrame = {
    import devicesDataDF.sparkSession.implicits._

    devicesDataDF
      .select(col(columnName).as("id"), $"bytes")
      .groupBy($"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit(metricName))
      .withColumn("timestamp", lit(filterDate.toEpochSecond).cast(TimestampType))
  }

  def writeToJdbc(devicesDataDF: DataFrame, jdbcURI: String, tableName: String): Future[Unit] = Future {
    devicesDataDF
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", tableName)
      .option("user", "postgres")
      .option("password", "keepcoding")
      .save()
  }

  def main(args: Array[String]): Unit = {
    val JdbcURI = args(0)
    val StorageURI = args(1)
    val filterDate = OffsetDateTime.parse(args(2))

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("StreamingAnalyticsJob")
      .getOrCreate()

    import spark.implicits._

    val userMetadataDF = spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", JdbcURI)
      .option("dbtable", "user_metadata")
      .option("user", "postgres")
      .option("password", "keepcoding")
      .load()

    val devicesDataDF = spark
      .read
      .format("parquet")
      .option("path", s"${StorageURI}/data")
      .load()
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
      .persist()

    val userTotalBytesDF = computeTotalBytesAgg(devicesDataDF, "id", "user_bytes_total", filterDate).persist()
    val userQuotaLimitDF = userTotalBytesDF.as("user").select($"id", $"value")
      .join(
        userMetadataDF.select($"id", $"email", $"quota").as("metadata"),
        $"user.id" === $"metadata.id" && $"user.value" > $"metadata.quota"
      )
      .select($"metadata.email", $"user.value".as("usage"), $"metadata.quota", lit(filterDate.toEpochSecond).cast(TimestampType).as("timestamp"))

    Await.result(
      Future.sequence(Seq(
        writeToJdbc(computeTotalBytesAgg(devicesDataDF, "antenna_id", "antenna_bytes_total", filterDate), JdbcURI, "bytes_hourly"),
        writeToJdbc(computeTotalBytesAgg(devicesDataDF, "app", "app_bytes_total", filterDate), JdbcURI, "bytes_hourly"),
        writeToJdbc(userTotalBytesDF, JdbcURI, "bytes_hourly"),
        writeToJdbc(userQuotaLimitDF, JdbcURI, "user_quota_limit")
      )), Duration.Inf
    )
  }
}
