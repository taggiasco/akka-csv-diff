package ch.taggiasco.streams.csv

object DiffExecute {
  
  private def findData(
    elements: Seq[Map[String, String]],
    column:   String,
    value:    String
  ): Option[Map[String, String]] = {
    elements.find(element => {
      element.exists(kv => {
        kv._1 == column && kv._2 == value
      })
    })
  }
  
  
  private def verify(
    result: DiffResult,
    key:    String,
    source: Map[String, String],
    target: Map[String, String]
  ): DiffResult = {
    val res = source.foldLeft((result, true))((currentResult, currentElement) => {
      target.get(currentElement._1) match {
        case Some(v) if v == currentElement._2 =>
          // same data
          currentResult
        case Some(v) =>
          // different data
          (currentResult._1.addDiffByColForKey(currentElement._1, key), false)
        case None =>
          // not existing data
          (currentResult._1.addDiffByColForKey(currentElement._1, key), false)
      }
    })
    if(res._2) {
      res._1.newLineOK
    } else {
      res._1.newLineKO.addKoLine(key)
    }
  }
  
  
  def compare(
    config:  CsvDiffConfig,
    sources: Seq[Map[String, String]],
    targets: Seq[Map[String, String]]
  ): DiffResult = {
    sources.foldLeft(DiffResult())((currentResult, source) => {
      val result = currentResult.addLine
      source.get(config.keyColumnName) match {
        case Some(value) if value.nonEmpty =>
          findData(targets, config.keyColumnName, value) match {
            case Some(target) =>
              // check the two lines
              verify(result, value, source, target)
            case None =>
              // the line is missing
              result.newMissingLine.addMissingLine(value)
          }
        case _ =>
          // if empty or not defined
          result.newLineWithNoKey
      }
    })
  }
  
}