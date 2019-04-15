package tomas.dvorak.analytics

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import tomas.dvorak.analytics.model.SegmentEvent

import scala.collection.JavaConverters._


class Job(kafkaBroker: String, windowGap: Time) {

  def defineExecutionPlan(env: StreamExecutionEnvironment): Unit = {

    val input = {
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", kafkaBroker)
      new FlinkKafkaConsumer(Seq("input").asJava, new SimpleStringSchema, properties)
        .setStartFromEarliest()
    }

    val output = new FlinkKafkaProducer(kafkaBroker, "output", new TaskMetricSerializationSchema)

    env
      .addSource(input)
      .flatMap(SegmentEvent.fromLine(_))
      .keyBy(_.taskId)
      .window(ProcessingTimeSessionWindows.withGap(windowGap))
      .aggregate(new TaskMetricAggregate)
      .addSink(output)
  }

}
