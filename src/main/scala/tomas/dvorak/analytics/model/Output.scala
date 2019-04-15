package tomas.dvorak.analytics.model


case class TaskMetric(taskId: String, confirmedSegments: Long)


case class TaskModel(taskId: String, confirmedSegmentIds: Set[String]) {

  def toMetric: TaskMetric = TaskMetric(taskId, confirmedSegmentIds.size)

  def add(event: SegmentEvent): TaskModel = TaskModel(
    event.taskId,
    if (event.confirmed) {
      confirmedSegmentIds + event.tUnitId
    } else {
      confirmedSegmentIds - event.tUnitId
    }
  )

}
