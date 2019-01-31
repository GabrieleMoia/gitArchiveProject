package classes
import java.lang.{Boolean => JBoolean}

case class Commits(sha: String, author: Author, message: String, distinct: JBoolean, url: String)
