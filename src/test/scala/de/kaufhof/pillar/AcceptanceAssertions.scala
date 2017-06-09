package de.kaufhof.pillar

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Metadata, Session}
import org.scalatest.Matchers

trait AcceptanceAssertions extends Matchers {
  val session: Session
  val keyspaceName: String

  protected def assertEmptyAppliedMigrationsTable(appliedMigrationsTableName: String = "applied_migrations") {
    session.execute(QueryBuilder.select().from(keyspaceName, appliedMigrationsTableName)).all().size() should equal(0)
  }

  protected def assertKeyspaceDoesNotExist() {
    val metadata: Metadata = session.getCluster.getMetadata
    metadata.getKeyspace(keyspaceName) should be(null)
  }
}
