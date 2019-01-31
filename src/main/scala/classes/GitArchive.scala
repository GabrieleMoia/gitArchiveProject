package classes
import java.lang.{Boolean => JBoolean}

case class GitArchive(id: String, `type`: String, actor: Actor, repo: Repo, payload: Payload, public: JBoolean, created_at: String)
