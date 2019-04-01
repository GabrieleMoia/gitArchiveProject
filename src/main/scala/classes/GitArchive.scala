package classes

case class GitArchive(
  id: String,
  tipo: String,
  actor: Actor,
  repo: Repo,
  payload: Payload,
  publico: Boolean,
  created_at: String) extends Serializable
