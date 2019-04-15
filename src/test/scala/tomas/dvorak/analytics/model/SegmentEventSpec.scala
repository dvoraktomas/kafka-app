package tomas.dvorak.analytics.model

import org.scalatest.{Matchers, WordSpec}


class SegmentEventSpec extends WordSpec with Matchers {

  "SegmentEvent" when {

    "parsing op=c after" should {
      "produce event" in {
        val line = """{
            "op":"c",
            "ts_ms":1000,
            "after":"{\"taskId\":\"myTask\", \"levels\":[1], \"tUnits\":[{\"tUnitId\":\"mySegment\", \"confirmedLevel\":1}]}",
            "patch":null
          }"""
        SegmentEvent.fromLine(line) shouldBe Some(SegmentEvent("myTask", "mySegment", true))
      }
    }

    "parsing op=c patch" should {
      "produce event" in {
        val line = """{
            "op":"c",
            "ts_ms":1000,
            "patch":"{\"taskId\":\"myTask\", \"levels\":[1], \"tUnits\":[{\"tUnitId\":\"mySegment\", \"confirmedLevel\":1}]}",
            "after":null
          }"""
        SegmentEvent.fromLine(line) shouldBe Some(SegmentEvent("myTask", "mySegment", true))
      }
    }

    "parsing op=d" should {
      "produce nothing" in {
        val line = """{"op":"d", "ts_ms":1000}"""
        SegmentEvent.fromLine(line) shouldBe None
      }
    }

    "parsing op=c without patch or after" should {
      "fail" in {
        val line = """{"op":"c", "ts_ms":1000}"""
        an[Exception] should be thrownBy SegmentEvent.fromLine(line)
      }
    }
  }

}
