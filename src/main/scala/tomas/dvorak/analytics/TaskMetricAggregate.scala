package tomas.dvorak.analytics

import org.apache.flink.api.common.functions.AggregateFunction
import tomas.dvorak.analytics.model.{SegmentEvent, TaskMetric, TaskModel}


class TaskMetricAggregate extends AggregateFunction[SegmentEvent, TaskModel, TaskMetric] {

  override def createAccumulator(): TaskModel = TaskModel("", Set())

  override def add(event: SegmentEvent, accumulator: TaskModel): TaskModel = accumulator.add(event)

  override def getResult(accumulator: TaskModel): TaskMetric = accumulator.toMetric

  override def merge(a: TaskModel, b: TaskModel): TaskModel = TaskModel(a.taskId, a.confirmedSegmentIds ++ b.confirmedSegmentIds)
}
