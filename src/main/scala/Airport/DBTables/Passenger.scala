package Airport.DBTables

import slick.jdbc.PostgresProfile.api._

case class Passenger(IDPsg: Option[Int],
                     name: String)

class PassengerTable(tag: Tag) extends Table[Passenger](tag, "Passenger") {
  def IDPsg = column[Int]("ID_psg", O.PrimaryKey, O.AutoInc)
  def name  = column[String]("name")

//  def

  def * = (IDPsg.?, name).mapTo[Passenger]
}
