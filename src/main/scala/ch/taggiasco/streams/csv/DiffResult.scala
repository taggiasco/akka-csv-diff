package ch.taggiasco.streams.csv


case class DiffResult(
  okLines:       Int,
  koLines:       Int,
  missingLines:  Int,
  lineWithNoKey: Int,
  diffByCols:    Map[String, Int]
) {
  
  override def toString: String = {
    s"""Number of lines OK : $okLines
        Number of lines KO : $koLines
        Number of missing lines : $missingLines
    """.stripMargin
  }
  
  def newLineOK: DiffResult = this.copy(okLines = okLines+1)
  
  def newLineKO: DiffResult = this.copy(koLines = koLines+1)
  
  def newMissingLine: DiffResult = this.copy(missingLines = missingLines+1)
  
  def newLineWithNoKey: DiffResult = this.copy(lineWithNoKey = lineWithNoKey+1)
  
  def addDiffByCol(name: String): DiffResult = {
    val n = diffByCols.get(name).getOrElse(0) + 1
    val diffs = diffByCols + (name -> n)
    this.copy(diffByCols = diffs)
  }
  
}



object DiffResult {
  
  def apply(): DiffResult = {
    DiffResult(0, 0, 0, 0, Map.empty[String, Int])
  }
  
}
