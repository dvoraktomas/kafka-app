package tomas.dvorak.analytics

import net.manub.embeddedkafka.{Consumers, EmbeddedKafka}
import net.manub.embeddedkafka.Codecs._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalatest._

import scala.io.Source


class JobSpec extends WordSpec with EmbeddedKafka with Consumers with Matchers {

  var testData: Iterator[String] = _


  "Job" when {
    "receives sample data" should {
      "output 2 records" in {
        testData.foreach(publishStringMessageToKafka("input", _))
        consumeNumberKeyedMessagesFrom[String, String]("output", 2) should contain allOf (
          "TyazVPlL11HYaTGs1_dc1" -> "2",
          "jNazVPlL11HFhTGs1_dc1" -> "0"
        )
      }
    }
  }


  override def withFixture(test: NoArgTest) = {

    val source = Source.fromFile("src/test/resources/kafka-messages.jsonline")
    testData = source.getLines

    val jobExecution = new Thread {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val job = new Job("localhost:6001", Time.seconds(1))
        job.defineExecutionPlan(env)
        env.execute()
      }
    }

    withRunningKafka {
      jobExecution.start()
      try super.withFixture(test)
      finally {
        jobExecution.interrupt()
        source.close()
      }
    }
  }

}
