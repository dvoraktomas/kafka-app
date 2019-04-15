package tomas.dvorak.analytics.model

import com.typesafe.scalalogging.StrictLogging

import scala.util.parsing.json.JSON


object SegmentEvent {

  JSON.globalNumberParser = BigDecimal(_)

  def fromLine(line: String): Option[SegmentEvent] = new JsonLine(line).eventOpt.flatMap(_.segmentEvent)

}


private class JsonLine(line: String) extends StrictLogging {

  private val value = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]
  private val op = value("op")
  logger.trace("op = {} at {}", op, value("ts_ms"))

  private def event = {
    val jsonBody = Iterator("patch", "after").map(value(_)).find(_ != null).get
    new Event(jsonBody.asInstanceOf[String])
  }

  def eventOpt: Option[Event] = {
    op match {
      case "c" => Some(event)
      case _ => None
    }
  }

}


private class Event(jsonBody: String) extends StrictLogging {

  private val value = JSON.parseFull(jsonBody).get.asInstanceOf[Map[String, Any]]

  private val taskId: String = value("taskId").asInstanceOf[String]
  logger.trace("taskId = {}", taskId)

  private val levels = value("levels").asInstanceOf[List[BigDecimal]]
  logger.trace("levels = {}", levels)

  private val segment = value("tUnits").asInstanceOf[List[Map[String, _]]](0)
  private val tUnitId = segment("tUnitId").asInstanceOf[String]
  logger.trace("tUnitId = {}", tUnitId)

  private def confirmedLevel0 = {
    val confirmedLevel0 = segment("confirmedLevel").asInstanceOf[BigDecimal]
    logger.trace("tUnits[0].confirmedLevel = {}", confirmedLevel0)
    confirmedLevel0
  }

  private def confirmed: Boolean = {
    val confirmed = levels(0) == confirmedLevel0
    logger.trace("confirmed = {}", confirmed)
    confirmed
  }

  def segmentEvent: Option[SegmentEvent] =
    if (levels.nonEmpty) Some(SegmentEvent(taskId, tUnitId, confirmed)) else None

}


case class SegmentEvent(taskId: String, tUnitId: String, confirmed: Boolean)

