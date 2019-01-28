package ch.taggiasco.streams.csv

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption



case class DiffResultWriter private(name: String) {
  private val dateFormater = new SimpleDateFormat("dd.MM.yyyy hh.mm")
  
  private val file = {
    val f = new File(name)
    if(!f.exists()) {
      f.createNewFile()
    }
    f
  }
  def write(msg: String) {
    Files.write(file.toPath, (msg + "\n").getBytes(), StandardOpenOption.APPEND)
  }
  def writeCurrentTimestamp(prefix: String) {
    val date = dateFormater.format(Calendar.getInstance().getTime())
    write(prefix + date)
  }
}


object DiffResultWriter {
  
  private val dateFormater = new SimpleDateFormat("dd.MM.yyyy_hh.mm")
  
  def apply(name: String): DiffResultWriter = {
    val date   = dateFormater.format(Calendar.getInstance().getTime())
    val writer = new DiffResultWriter(s"${name}_${date}.log")
    writer.writeCurrentTimestamp("Starting at ")
    writer
  }
  
}
