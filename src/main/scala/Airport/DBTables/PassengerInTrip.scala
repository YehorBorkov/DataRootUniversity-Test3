package Airport.DBTables

import slick.jdbc.PostgresProfile.api._
import java.time.Instant

case class PassengerInTrip(tripNo: Int,
                           date: Instant,
                           IDPsg: Int,
                           place: String)

class PassengerInTripTable(tag: Tag) extends Table[PassengerInTrip](tag, "Pass_in_trip") {
  def tripNo = column[Int]("trip_no")
  def date   = column[Instant]("date")
  def IDPsg  = column[Int]("ID_Psg")
  def place  = column[String]("place")

  def tripNoFk = foreignKey("trip_no_fk", tripNo, trips)(_.tripNo)
  def IDPsgFk  = foreignKey("ID_Psg_fk", IDPsg, passengers)(_.IDPsg)

  def * = (tripNo, date, IDPsg, place).mapTo[PassengerInTrip]
}
