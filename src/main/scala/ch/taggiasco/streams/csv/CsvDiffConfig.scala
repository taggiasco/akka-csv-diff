package ch.taggiasco.streams.csv

import com.typesafe.config.Config
import scala.collection.JavaConverters._


case class CsvDiffConfig(
  name:                  String,
  originFilename:        String,
  targetFilename:        String,
  columns:               Int,
  keyColumns:            List[Int],
  keyColumnNames:        List[String],
  removeDuplicateSpaces: Boolean,
  removeCarriageReturns: Boolean,
  ignoreHeaderLine:      Boolean,
  columnsToIgnore:       List[String],
  columnsToIgnoreNull:   List[String],
  columnsToRemoveSpace:  List[String]
) {
  require(keyColumns.size > 0)
  keyColumns.foreach(key => require(key <= columns))
}


object CsvDiffConfig {
  
  private def getListOfColumns(config: Config, name: String, columnPrefix: String): List[String] = {
    try {
      val cols = config.getIntList(name).asScala.toList.map(_.toInt)
      cols.map(n => columnPrefix + n)
    } catch {
      case e: Exception =>
        List.empty[String]
    }
  }
  
  
  def apply(name: String, columnPrefix: String)(implicit config: Config): CsvDiffConfig = {
    val conf         = config.getConfig("csv-diff").getConfig(name)
    val keys         = conf.getIntList("keyColumn").asScala.toList.map(_.toInt)
    val names        = keys.map(v => columnPrefix + v)
    
    CsvDiffConfig(
      name,
      conf.getString("originFilename"),
      conf.getString("targetFilename"),
      conf.getInt("columns"),
      keys,
      names,
      conf.getBoolean("removeDuplicateSpaces"),
      conf.getBoolean("removeCarriageReturns"),
      conf.getBoolean("ignoreHeaderLine"),
      getListOfColumns(conf, "columnsToIgnore", columnPrefix),
      getListOfColumns(conf, "columnsToIgnoreNull", columnPrefix),
      getListOfColumns(conf, "columnsToRemoveSpace", columnPrefix)
    )
  }
}
