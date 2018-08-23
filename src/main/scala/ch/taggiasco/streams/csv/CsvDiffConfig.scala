package ch.taggiasco.streams.csv

import com.typesafe.config.Config


case class CsvDiffConfig(
  name:           String,
  originFilename: String,
  targetFilename: String,
  columns:        Int
)


object CsvDiffConfig {
  def apply(name: String)(implicit config: Config): CsvDiffConfig = {
    val conf = config.getConfig("csv-diff").getConfig(name)
    CsvDiffConfig(name, conf.getString("originFilename"), conf.getString("targetFilename"), conf.getInt("columns"))
  }
}
