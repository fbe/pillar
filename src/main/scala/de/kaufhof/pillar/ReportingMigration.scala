package de.kaufhof.pillar

import java.util.Date
import com.datastax.driver.core.Session

class ReportingMigration(reporter: Reporter, wrapped: Migration) extends Migration {
  val description: String = wrapped.description
  val authoredAt: Date = wrapped.authoredAt
  val up: Seq[String] = wrapped.up

  override def executeUpStatement(session: Session, appliedMigrationsTableName: String) {
    reporter.applying(wrapped)
    wrapped.executeUpStatement(session, appliedMigrationsTableName)
  }

  def executeDownStatement(session: Session, appliedMigrationsTableName: String) {
    reporter.reversing(wrapped)
    wrapped.executeDownStatement(session, appliedMigrationsTableName)
  }
}
