package tomas.dvorak.analytics

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object Launcher {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val job = new Job(params.get("kafkaBroker"), Time.minutes(params.getLong("windowGapMinutes")))
    job.defineExecutionPlan(env)
    env.execute("Analytics")
  }
}
