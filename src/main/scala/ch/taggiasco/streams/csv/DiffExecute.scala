package ch.taggiasco.streams.csv

object DiffExecute {
  
  
  private def getKeyValues(
    element: Map[String, String],
    keys:    List[String]
  ): Map[String, String] = {
    val result = keys.foldLeft((Map.empty[String, String], true))((acc, keyName) => {
      if(acc._2) {
        // for instance, it's valid
        element.get(keyName) match {
          case Some(value) if value.nonEmpty =>
            // still valid
            (acc._1 + (keyName -> value), true)
          case _ =>
            // becomes invalid
            (acc._1, false)
        }
      } else {
        acc
      }
    })
    if(result._2) {
      // if the result is valid
      result._1
    } else {
      // if it's not, returns empty map
      Map.empty[String, String]
    }
  }
  
  
  private def buildStringValue(
    values:   Map[String, String]
  ): String = {
    if(values.size == 1) {
      values.head._2
    } else {
      "(" + values.toSeq.sortBy(_._1).map(_._2).mkString(", ") + ")"
    }
  }
  
  
  private def matchData(
    element: Map[String, String],
    values:  Map[String, String]
  ): Boolean = {
    values.foldLeft(true)((acc, value) => {
      if(acc) {
        element.get(value._1) match {
          case Some(v) if v == value._2 => true
          case _ => false
        }
      } else {
        // if already failed
        acc
      }
    })
  }
  
  
  private def findData(
    elements: Seq[Map[String, String]],
    values:   Map[String, String]
  ): Option[Map[String, String]] = {
    elements.find(element => matchData(element, values))
  }
  
  
//  private def findData(
//    elements: Seq[Map[String, String]],
//    column:   String,
//    value:    String
//  ): Option[Map[String, String]] = {
//    elements.find(element => {
//      element.exists(kv => {
//        kv._1 == column && kv._2 == value
//      })
//    })
//  }
  
  
  private def removeSpaces(
    config: CsvDiffConfig,
    column: String
  )(
    value:  String
  ): String = {
    if(config.columnsToRemoveSpace.contains(column)) {
      value.replaceAll(" ", "")
    } else {
      value
    }
  }
  
  
  private def verify(
    config:  CsvDiffConfig,
    result:  DiffResult,
    key:     String,
    source:  Map[String, String],
    target:  Map[String, String]
  ): DiffResult = {
    //val indexedSource = source.zipWithIndex.map(t => (t._1, t._2+1))
    val res = source.foldLeft((result, true))((currentResult, currentElement) => {
      val adapter = removeSpaces(config, currentElement._1) _
      if(config.columnsToIgnore.contains(currentElement._1)) {
        // data to ignore, so we don't count it
        currentResult
      } else if(config.columnsToIgnoreNull.contains(currentElement._1) && target.get(currentElement._1).getOrElse("") == "") {
        // data to ignore if null, and it seems to be the case
        currentResult
      } else {
        val value = target.get(currentElement._1).map(adapter)
        value match {
          case Some(v) if v == adapter(currentElement._2) =>
            // same data
            currentResult
          case Some(v) =>
            // different data
            (currentResult._1.addDiffByColForKey(currentElement._1, key), false)
          case None =>
            // not existing data
            (currentResult._1.addDiffByColForKey(currentElement._1, key), false)
        }
      }
    })
    if(res._2) {
      res._1.newLineOK
    } else {
      res._1.newLineKO.addKoLine(key)
    }
  }
  
  
  private def removeDuplicateSpaces(s: String): String = s.trim().replaceAll(" +", " ")
  
  
  private def removeCarriageReturns(s: String): String = s.replaceAll("\n", " ")
  
  
  private def reformat(
    config:  CsvDiffConfig,
    value:   String
  ): String = {
    val newValue = {
      if(config.removeCarriageReturns) {
        removeCarriageReturns(value)
      } else {
        value
      }
    }
    if(config.removeDuplicateSpaces) {
      removeDuplicateSpaces(newValue)
    } else {
      newValue
    }
  }
  
  
  private def reformat(
    config:  CsvDiffConfig,
    elements: Seq[Map[String, String]]
  ): Seq[Map[String, String]] = {
    if(config.removeCarriageReturns || config.removeDuplicateSpaces) {
      elements.map(elem => {
        elem.map(nv => {
          nv._1 -> reformat(config, nv._2)
        })
      })
    } else {
      elements
    }
  }
  
  
  private def compare0(
    config:  CsvDiffConfig,
    sources: Seq[Map[String, String]],
    targets: Seq[Map[String, String]]
  ): DiffResult = {
    val actives = {
      if(config.ignoreHeaderLine) {
        sources.tail
      } else {
        sources
      }
    }
    actives.foldLeft(DiffResult())((currentResult, source) => {
      val result = currentResult.addLine
      val values = getKeyValues(source, config.keyColumnNames)
      if(values.nonEmpty) {
        // entry is valid
        val value = buildStringValue(values)
        findData(targets, values) match {
          case Some(target) =>
            // check the two lines
            verify(config, result, value, source, target)
          case None =>
            // the line is missing
            result.newMissingLine.addMissingLine(value)
        }
      } else {
        // entry is not valid
        result.newLineWithNoKey
      }
      
//      source.get(config.keyColumnNames.head) match {
//        case Some(value) if value.nonEmpty =>
//          findData(targets, config.keyColumnNames.head, value) match {
//            case Some(target) =>
//              // check the two lines
//              verify(result, value, source, target)
//            case None =>
//              // the line is missing
//              result.newMissingLine.addMissingLine(value)
//          }
//        case _ =>
//          // if empty or not defined
//          result.newLineWithNoKey
//      }
    })
  }
  
  
  def compare(
    config:  CsvDiffConfig,
    sources: Seq[Map[String, String]],
    targets: Seq[Map[String, String]]
  ): DiffResult = {
    compare0(
      config,
      reformat(config, sources),
      reformat(config, targets)
    )
  }
  
}