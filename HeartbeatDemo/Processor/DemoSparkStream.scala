package com.microsoft.azurearcdata.sparkmsit.etl.drivers.demos

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object DemoSparkStream extends App with Logging {

  val Array(heartbeatConnectionStringBase64, stateConnectionStringBase64, checkpointRoot) = args

  val heartbeatConnectionString = new String(java.util.Base64.getDecoder.decode(heartbeatConnectionStringBase64))
  val stateConnectionString = new String(java.util.Base64.getDecoder.decode(stateConnectionStringBase64))

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName(this.getClass.getSimpleName.stripSuffix("$"))
    .getOrCreate()

  val heartbeatConf =
    EventHubsConf(heartbeatConnectionString).setStartingPosition(
      EventPosition.fromEndOfStream
    )
  val stateConf = EventHubsConf(stateConnectionString)

  import spark.implicits._

  val heartbeatDF = spark.readStream
    .format("eventhubs")
    .options(heartbeatConf.toMap)
    .load()
    .selectExpr("sequenceNumber", "enqueuedTime", "CAST(body AS STRING)")
    .withColumn(
      "bodyStruct",
      from_json(
        col("body"),
        StructType(
          Array(
            StructField("machine_name", StringType, true),
            StructField("machine_time", StringType, true)
          )
        )
      )
    )
    .selectExpr("*", "bodyStruct.*")
    .selectExpr("enqueuedTime AS eventTime", "machine_name AS machineName")
    .as[MachineHeartbeat]
    .withWatermark("eventTime", "30 seconds")
    .groupByKey(_.machineName)
    .flatMapGroupsWithState(
      OutputMode.Update,
      GroupStateTimeout.EventTimeTimeout
    )(heartbeatStateTransitionFunc)
    .toDF("machine_name", "last_status_change_time", "status")

  val heartbeatEhQuery = heartbeatDF
    .selectExpr("to_json(struct(*)) as body")
    .writeStream
    .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
    .outputMode("update")
    .options(stateConf.toMap)
    .option("checkpointLocation", s"${checkpointRoot}/heartbeat_eh")
    .trigger(Trigger.ProcessingTime("0 seconds"))
    .queryName("heartbeat_eh")
    .start()

  spark.streams.awaitAnyTermination()

  // ============================================================ //

  /** Performs state transition logic for updating instance status based on
    * incoming heartbeats.
    *
    * @param machineName
    *   The unique identifier for the SQL Server Machine.
    * @param values
    *   An iterator of incoming, unprocessed heartbeats for that particular SQL
    *   Server Machine.
    * @param state
    *   The Spark [[GroupState]] used to manage and persist SQL Server Machine
    *   state.
    * @return
    *   An iterator of [[MachineStatus]] representing the updated SQL Server
    *   Machine status.
    */
  def heartbeatStateTransitionFunc(
      machineName: String,
      values: Iterator[MachineHeartbeat],
      state: GroupState[MachineState]
  ): Iterator[MachineStatus] = {

    // Configurable heartbeat timeout grace period
    //
    val heartbeatGracePeriodTimeoutMilliseconds = 1000

    // Hit due to Spark triggered timeout due to lack of events - Machine is offline
    //
    if (state.hasTimedOut) {

      logWarning(
        s"[${Timestamp.from(Instant.now())}] ${machineName} has timed out, removing state."
      )
      state.remove()

      Iterator(
        MachineStatus(
          machineName,
          Timestamp.from(Instant.now()),
          HeartbeatStatus.Unhealthy.toString
        )
      )

    } else if (!state.exists) {
      logInfo(
        s"[${Timestamp.from(Instant.now())}] ${machineName} initializing state."
      )

      state.setTimeoutTimestamp(
        state.getCurrentWatermarkMs() + heartbeatGracePeriodTimeoutMilliseconds
      )

      state.update(MachineState(HeartbeatStatus.Initializing.toString))

      return Iterator(
        MachineStatus(
          machineName,
          Timestamp.from(Instant.now()),
          HeartbeatStatus.Initializing.toString
        )
      )
    } else {

      state.setTimeoutTimestamp(
        state.getCurrentWatermarkMs() + heartbeatGracePeriodTimeoutMilliseconds
      )

      // Attempt to obtain previously persisted state
      //
      val previousHeartbeatStatus = state.getOption
        .map(_.heartbeatStatus)
        .getOrElse(HeartbeatStatus.Unhealthy.toString)

      // Take the latest heartbeat in the iterator, we don't care about intermediate events, only the latest heartbeat matters
      //
      val latestHeartbeat = values.maxBy(_.eventTime.getTime)

      // Calculate current state
      //
      val currentHeartbeatStatus = HeartbeatStatus.Healthy.toString

      // Persist state, for next transition
      //
      state.update(MachineState(currentHeartbeatStatus))

      // If previous and current status are equal, yield empty iterator to reduce load
      //
      if (previousHeartbeatStatus == currentHeartbeatStatus) {
        Iterator.empty
      }
      // Yield updated state change to trigger UPSERT
      //
      else {
        logInfo(
          s"[${Timestamp.from(Instant.now())}] ${machineName} yielding new payload, state changed"
        )
        Iterator(
          MachineStatus(
            machineName,
            Timestamp.from(Instant.now()),
            currentHeartbeatStatus
          )
        )
      }
    }
  }

  // ============================================================ //
}

case class MachineStatus(
    machineName: String,
    lastStatusChangeTime: Timestamp,
    status: String
)

case class MachineState(
    heartbeatStatus: String
)

case class MachineHeartbeat(
    eventTime: Timestamp,
    machineName: String
)

object HeartbeatStatus extends Enumeration {
  type HeartbeatStatus = Value
  val Healthy, Initializing, Unhealthy = Value
}
