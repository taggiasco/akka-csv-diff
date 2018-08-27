package ch.taggiasco.streams.csv

import java.nio.file.Paths
import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.stream.ActorMaterializer
import akka.stream.IOResult
import akka.stream.scaladsl._
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}

import scala.util.{Try, Failure, Success}
import scala.concurrent.Future

import com.typesafe.config.ConfigFactory


object DiffRunner {
  
  private case class EndException(msg: String) extends Exception(msg)
  
  
  private def endApp(msg: String)(implicit system: ActorSystem): Unit = {
    system.terminate()
    throw EndException(msg)
  }
  
  
  def main(args: Array[String]) {
    try {
      // actor system and implicit materializer
      implicit val system = ActorSystem("system")
      implicit val materializer = ActorMaterializer()
      implicit val executionContext = materializer.executionContext
      implicit val configuration = ConfigFactory.load()
      val columnPrefix = "column_"
      
      val config = CsvDiffConfig(args.head, columnPrefix)
      
      val columns = (1 to config.columns map { n => columnPrefix+n }).toSeq
      
      val originGraph = CsvDiffParser.parse(CsvDiffFile.load(config.originFilename), columns)
      val targetGraph = CsvDiffParser.parse(CsvDiffFile.load(config.targetFilename), columns)
      
      val futures = List(originGraph, targetGraph)
      Future.sequence(futures).onComplete {
        case Success(results) =>
          val originResults = results(0)
          val targetResults = results(1)
          val result = DiffExecute.compare(config, originResults, targetResults)
//          println("Origin results: ")
//          println(originResults)
//          println("Target results: ")
//          println(targetResults)
//          println("----")
          println("Results:")
          println("--------")
          println(result)
          system.terminate()
        case Failure(e) =>
          println(s"Failure: ${e.getMessage}")
          system.terminate()
      }
    } catch {
      case EndException(msg) =>
        println(s"Failure: $msg")
    }
  }
}
