import java.time._

package object Airport {
  implicit class implicitInstantConversions(instant: Instant) {
    def toLocalDateTime: LocalDateTime = java.time.LocalDateTime.from(instant.atZone(ZoneId.of("GMT")))
    def toLocalDate:     LocalDate     = toLocalDateTime.toLocalDate
    def toLocalTime:     LocalTime     = toLocalDateTime.toLocalTime
  }
}
