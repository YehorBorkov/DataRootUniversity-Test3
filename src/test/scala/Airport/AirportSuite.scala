package Airport

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import slick.jdbc.PostgresProfile.api._
import Airport.DBTables._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class AirportSuite extends FunSuite {
  val db = Database.forConfig("airport")
  def exec[T](action: DBIO[T]): T = {
    Await.result(db.run(action), Duration.Inf)
  }

  test("task 63") {
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      groupBy{ case(passenger, _) => passenger.name }.
      map{ case(passengerName, hisTrips) => (passengerName, hisTrips.map(_._2).map(_.place).countDistinct, hisTrips.length) }.
      filter{ case(_, distinctPlacesCount, placesCount) => distinctPlacesCount =!= placesCount }.
      map{ case(name, _, _) => name }.
      result
    /* For some implicit reason using .disticnt.lenght query produces slick.SlickTreeException: Cannot convert node to SQL Comprehension
       So I run deprecated .countDistinct in query instead. Kind of the same. Slick is buggy as hell #1 */
    assert(exec(query).equals(Vector("Bruce Willis", "Mullah Omar", "Nikole Kidman")))
  }

  test("task 67") {
    val query = trips.
      map( trip => (trip.townFrom, trip.townTo) ).
      groupBy{ case(townFrom, townTo) => (townFrom, townTo) }.
      map{ case(_, count) => count.length }.
      result
    /* For some implicit reason adding .max to query itself produces slick.SlickTreeException: Cannot convert node to SQL Comprehension
       So I run .max on resulting collection instead. Kind of the same. Slick is buggy as hell #2 */
    val result = exec(query)
    val max = result.max
    assert(result.count(_ == max) == 4)
  }

  test("task 68") {
    val query = trips.join(trips).on( (trip1, trip2) => trip2.townFrom === trip1.townTo && trip2.townTo === trip1.townFrom).
      map{ case (trip1, _) => (trip1.townFrom, trip1.townTo) }.
      result
    /* Slick is kind of cool, but lacks some interactive features, such as SQL's CASE and conditional actions.
       Still, I can ger all the tuples i need and process them within Scala. Kind of the same. Slick is clunky as hell */
    val result = exec(query).
      map{ case(from, to) => if (from > to) from + to else to + from }.
      groupBy{ string => string }.
      map{ case(_, allStrings) => allStrings.length }
    val max = result.max
    assert(result.count(_ == max) == 2)
  }

  test("task 72") {
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      join(trips).on{ case((_, passengerInTrip), trip) => passengerInTrip.tripNo === trip.tripNo }.
      join(companies).on{ case(((_, _), trip), company) => trip.IDComp === company.IDComp }.
      map{ case(((passenger, _), _), company) => (passenger.name, company.name) }.
      groupBy{ case(name, company) => (name, company) }.
      map{ case((name, _), flights) => (name, flights.length) }.
      result
    /* It's actually really tricky to get this working using Slick-only.
       I believe, some Scala post-processing is okay. Slick seems more friendly the more time you invest. Still, somehow strange */
    val queryResult = exec(query)
    val max = queryResult.map{ case(_, amount) => amount }.max
    val result = queryResult.filter{ case(_, amount) => amount == max }
    assert(result == Vector(("Mullah Omar", 4), ("Michael Caine", 4)))
  }

}
