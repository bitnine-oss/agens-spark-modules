package net.bitnine.agens.cypher.api.io.fs

import net.bitnine.agens.cypher.api.CAPSSession


trait FileSystemOperations[Path] {

  implicit val session: CAPSSession

  protected def listDirectories(path: Path): List[String]

  protected def deleteDirectory(path: Path): Unit

  protected def readFile(path: Path): String

  protected def writeFile(path: Path, content: String): Unit

}
