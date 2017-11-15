package Airport.DBTables

import slick.jdbc.PostgresProfile.api._

case class Company(IDComp: Option[Int],
                   name: String)

class CompanyTable(tag: Tag) extends Table[Company](tag, "Company") {
  def IDComp = column[Int]("ID_comp", O.PrimaryKey, O.AutoInc)
  def name   = column[String]("name")

  def * = (IDComp.?, name).mapTo[Company]
}
