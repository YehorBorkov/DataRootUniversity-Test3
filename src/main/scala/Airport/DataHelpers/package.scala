package Airport

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

package object DataHelpers {
  val parser: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS")
  implicit def toInstant(date: String): Instant = {
    Instant.from(ZonedDateTime.of(LocalDateTime.from(parser.parse(date)), ZoneId.of("UTC")))
  }
}
