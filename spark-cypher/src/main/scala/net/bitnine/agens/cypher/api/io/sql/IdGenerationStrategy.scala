package net.bitnine.agens.cypher.api.io.sql


object IdGenerationStrategy extends Enumeration {
  type IdGenerationStrategy = Value

  val SerializedId, HashedId = Value
}
