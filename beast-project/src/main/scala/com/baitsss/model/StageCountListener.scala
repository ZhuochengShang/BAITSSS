package com.baitsss.model

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}

/**
 * A custom SparkListener that listens for stage submission events and logs information
 * about each stage as it is submitted to the Spark scheduler.
 */
class StageCountListener extends SparkListener {

  // Counter to track the number of stages submitted so far
  var stageCount = 0

  /**
   * This method is called automatically by Spark whenever a stage is submitted.
   *
   * The event parameter 'stageSubmitted' contains information about the submitted stage,
   * including its StageInfo and any associated properties.
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    // Increment the stage counter (how many stages have been submitted so far)
    stageCount += 1

    // --- Log details about the stage being submitted ---

    // This logs the full StageInfo object, which includes details such as:
    // - Stage ID
    // - Attempt number
    // - Number of tasks
    // - Parent stages, etc.
    println(s"[StageCountListener] StageInfo object submitted: ${stageSubmitted.stageInfo}")

    // This logs the properties map of the stage, often including metadata like:
    // - Locality preferences
    // - Job descriptions
    // - Any custom properties you might have set on this job
    println(s"[StageCountListener] Stage properties submitted: ${stageSubmitted.properties}")

    // This logs just the unique Stage ID, which is useful for identifying the stage:
    println(s"[StageCountListener] Stage ID ${stageSubmitted.stageInfo.stageId} submitted (count = $stageCount)")

    // This logs more human-readable details about the stage, such as:
    // - RDD lineage
    // - What operation triggered the stage (map, filter, etc.)
    println(s"[StageCountListener] Stage details: ${stageSubmitted.stageInfo.details}")

    // Optional: If you want to include other data (number of tasks, etc.)
    println(s"[StageCountListener] Number of tasks in this stage: ${stageSubmitted.stageInfo.numTasks}")
  }
}
