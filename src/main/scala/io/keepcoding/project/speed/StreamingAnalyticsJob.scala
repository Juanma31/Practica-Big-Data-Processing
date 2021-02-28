package io.keepcoding.project.speed

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object StreamingAnalyticsJob {

  def bytesTotalAgg(data: DataFrame, aggColumn: String, jdbcURI: String): Future[Unit] = Future {
    import data.sparkSession.implicits._

    data
      .select($"timestamp".cast(TimestampType).as("timestamp"), col(aggColumn), $"bytes")
      .withWatermark("timestamp", "6 minutes")
      .groupBy(window($"timestamp", "5 minutes"), col(aggColumn))
      .agg(sum($"bytes").as("total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        col(aggColumn).as("id"),
        $"total_bytes".as("value"),
        lit(s"${aggColumn}_total_bytes").as("type")
      )
      .writeStream
      .foreachBatch((dataset: DataFrame, batchId: Long) =>
        dataset
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", "bytes")
          .option("user", "postgres")
          .option("password", "keepcoding")
          .save()
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val KafkaServer = args(0)
    val JdbcURI = args(1)
    val StorageURI = args(2)

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("StreamingAnalyticsJob")
      .getOrCreate()

    import spark.implicits._

    val devicesSchema = StructType(Seq(
      StructField("timestamp", LongType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    ))

    val deviceStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaServer)
      .option("subscribe", "devices")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json($"value".cast(StringType), devicesSchema).as("json"))
      .select($"json.*")

    val storageStream = Future {
      deviceStream
        .select(
          $"id", $"antenna_id", $"bytes", $"app",
          year($"timestamp".cast(TimestampType)).as("year"),
          month($"timestamp".cast(TimestampType)).as("month"),
          dayofmonth($"timestamp".cast(TimestampType)).as("day"),
          hour($"timestamp".cast(TimestampType)).as("hour")
        )
        .writeStream
        .partitionBy("year", "month", "day", "hour")
        .format("parquet")
        .option("path", s"${StorageURI}/data")
        .option("checkpointLocation", s"${StorageURI}/checkpoint")
        .start()
        .awaitTermination()
    }

    val appStream = bytesTotalAgg(deviceStream, "app", JdbcURI)
    val userStream = bytesTotalAgg(deviceStream.withColumnRenamed("id", "user"), "user", JdbcURI)
    val antennaStream = bytesTotalAgg(deviceStream.withColumnRenamed("antenna_id", "antenna"), "antenna", JdbcURI)

    Await.result(Future.sequence(Seq(appStream, userStream, antennaStream, storageStream)), Duration.Inf)
  }
}
