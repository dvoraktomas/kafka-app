package tomas.dvorak.analytics.model

import org.scalatest.{Matchers, WordSpec}

import scala.util.parsing.json.JSON


class EventSpec extends WordSpec with Matchers {

  JSON.globalNumberParser = BigDecimal(_)

  "Event" when {

    "levels is empty" should {
      "produce nothing" in {
        val event = new Event("""{"taskId":"myTask", "levels":[], "tUnits":[{"tUnitId":"mySegment"}]}""")
        event.segmentEvent shouldBe None
      }
    }

    "levels[0] = tUnits[0].confirmedLevel" should {
      "produce SegmentEvent with confirm" in {
        val event = new Event("""{"taskId":"myTask", "levels":[123], "tUnits":[{"tUnitId":"mySegment", "confirmedLevel": 123}]}""")
        event.segmentEvent shouldBe Some(SegmentEvent("myTask", "mySegment" , confirmed = true))
      }
    }

    "levels[0] != tUnits[0].confirmedLevel" should {
      "produce SegmentEvent with unconfirm" in {
        val event = new Event("""{"taskId":"myTask", "levels":[123], "tUnits":[{"tUnitId":"mySegment", "confirmedLevel": 0}]}""")
        event.segmentEvent shouldBe Some(SegmentEvent("myTask", "mySegment" , confirmed = false))
      }
    }
  }

}
