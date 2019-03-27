package classes

case class GitArchive(
  id: String,
  `type`: String,
  actor: Actor,
  repo: Repo,
  payload: Payload,
  publico: Boolean,
  created_at: String)
