import Airport.DBTables._
import Airport.DataHelpers._

import scala.concurrent.Await
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.duration.Duration

object Main extends App {
  val db = Database.forConfig("airport")
  def exec[T](action: DBIO[T]): T = {
    Await.result(db.run(action), Duration.Inf)
  }

  val populateWithData1 = (companies forceInsertAll CompanyDataHelper.getData("Company.txt")) >>
    (passengers forceInsertAll PassengerDataHelper.getData("Passenger.txt")) >>
    (trips forceInsertAll TripDataHelper.getData("Trip.txt"))
  val populateWithData2 = passengersInTrips forceInsertAll PassengerInTripDataHelper.getData("Pass_in_trip.txt")

  val populateWithData = populateWithData1 andThen populateWithData2

//  exec(createSchema)
//  exec(populateWithData)

}
