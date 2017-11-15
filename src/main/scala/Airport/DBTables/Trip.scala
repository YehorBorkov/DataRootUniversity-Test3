package Airport.DBTables

import java.time.Instant

import slick.jdbc.PostgresProfile.api._


case class Trip(tripNo: Option[Int],
                IDComp: Int,
                plane: String,
                townFrom: String,
                townTo: String,
                timeOut: Instant,
                timeIn: Instant)

class TripTable(tag: Tag) extends Table[Trip](tag, "Trip") {
  def tripNo   = column[Int]("trip_no", O.PrimaryKey, O.AutoInc)
  def IDComp   = column[Int]("ID_comp")
  def plane    = column[String]("plane")
  def townFrom = column[String]("town_from")
  def townTo   = column[String]("town_to")
  def timeOut  = column[Instant]("time_out")
  def timeIn   = column[Instant]("time_in")

  def companyIdFk = foreignKey("ID_comp_fk", IDComp, companies)(_.IDComp)

  def * = (tripNo.?, IDComp, plane, townFrom, townTo, timeOut, timeIn).mapTo[Trip]
}
