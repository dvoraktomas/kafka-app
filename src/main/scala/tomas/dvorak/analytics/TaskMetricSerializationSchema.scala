package tomas.dvorak.analytics

import java.nio.charset.StandardCharsets

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import tomas.dvorak.analytics.model.TaskMetric


class TaskMetricSerializationSchema extends KeyedSerializationSchema[TaskMetric] {

  override def serializeKey(metric: TaskMetric): Array[Byte] = metric.taskId.getBytes(StandardCharsets.UTF_8)

  override def serializeValue(metric: TaskMetric): Array[Byte] = metric.confirmedSegments.toString.getBytes(StandardCharsets.UTF_8)

  override def getTargetTopic(metric: TaskMetric): String = null
}
