package demo

import java.nio.charset.StandardCharsets
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainApp {

  def lowerCaseTransformer(input: RDD[String]): RDD[String] =
    input.map(_.replaceAll("[,.!?:;]", "").toLowerCase)

  def createContext(projectID: String, windowLength: String, slidingInterval: String, checkpointDirectory: String)
    : StreamingContext = {

    // [START stream_setup]
    val sparkConf = new SparkConf().setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))

    // Set the checkpoint directory
    val yarnTags = sparkConf.get("spark.yarn.tags")
    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    ssc.checkpoint(checkpointDirectory + '/' + jobId)
    
    // Create stream
    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "mock_topic_studentInfo-sub",  // Cloud Pub/Sub subscription for incoming messages
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
    // [END stream_setup]

    //process the stream
    val processedMessagesStream: DStream[String] = messagesStream
      .window(Seconds(windowLength.toInt), Seconds(slidingInterval.toInt)) //create a window
      .transform(lowerCaseTransformer(_)) //apply transformation

    processedMessagesStream.print()

	ssc
  }

  def main(args: Array[String]): Unit = {
    System.err.println(args.toSeq)
    if (args.length != 5) {
      System.err.println(
        """
          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
          |
          |     <projectID>: ID of Google Cloud project
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |     <checkpointDirectory>: Directory used to store RDD checkpoint data
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectID, windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq

    // Create Spark context
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(projectID, windowLength, slidingInterval, checkpointDirectory))

    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
        ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}