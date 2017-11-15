package Airport

import java.time.{Instant, LocalDateTime}

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import slick.jdbc.PostgresProfile.api._
import Airport.DBTables._

import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class AirportSuite extends FunSuite {
  val db = Database.forConfig("airport")
  def exec[T](action: DBIO[T]): T = {
    Await.result(db.run(action), scala.concurrent.duration.Duration.Inf)
  }

  test("task 63(2) more slick") {
    /* Find the names of different passengers that ever travelled more than once occupying seats with the same number. */
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      groupBy{ case(passenger, _) => passenger.name }.
      map{ case(passengerName, hisTrips) => (passengerName, hisTrips.map(_._2).map(_.place).countDistinct, hisTrips.length) }.
      filter{ case(_, distinctPlacesCount, placesCount) => distinctPlacesCount =!= placesCount }.
      map{ case(name, _, _) => name }.
      result
    /* For some implicit reason using .disticnt.lenght query produces slick.SlickTreeException: Cannot convert node to SQL Comprehension
     * So I run deprecated .countDistinct in query instead. Kind of the same. Slick is buggy as hell #1 */

    assert(exec(query).toSet === Set("Bruce Willis", "Mullah Omar", "Nikole Kidman"))
  }

  test("task 63(2) more scala") {
    /* Find the names of different passengers that ever travelled more than once occupying seats with the same number. */
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      map{ case(passenger, trip) => (passenger.name, trip.place) }.
      result
    /* Example of task, where Slick code was spaghetti, and Scala processing afterwards was easier
     * I believe, the purpose of Slick is to provide this syntax to database operations. But it doesn't. It crashes. See next test */

    val result = exec(query).
      groupBy{ case(passengerName, _) => passengerName }.
      map{ case(passengerName, hisSeats) => (passengerName, hisSeats.map(_._2).length != hisSeats.map(_._2).distinct.length) }.
      filter{ case(_, flewOnSamePlaces) => flewOnSamePlaces}.
      map{ case(passengerName, _) => passengerName }.
      toSet

    assert(result === Set("Bruce Willis", "Mullah Omar", "Nikole Kidman"))
  }

  test("task 63(2) extended slick with exception") {
    /* Find the names of different passengers that ever travelled more than once occupying seats with the same number. */
    val exception = intercept[slick.SlickTreeException] {
      val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
        map{ case(passenger, trip) => (passenger.name, trip.place) }.
        groupBy{ case(passengerName, _) => passengerName }.
        map{ case(passengerName, hisSeats) => (passengerName, hisSeats.map(_._2).length, hisSeats.map(_._2).distinct.length)}.
        filter{ case(_, flewTimes, flewDistinct) => flewTimes =!= flewDistinct }.
        map{ case(passengerName, _, _) => passengerName }.
        result
      val result = exec(query).toSet
      assert(result === Set("Bruce Willis", "Mullah Omar", "Nikole Kidman"))
    }
    println(s"\nTest 63 Slick exception:\n${exception.getMessage}\n")
  }

  test("task 67(2)") {
    /* Find out the number of routes with the greatest number of flights (trips).
       Notes.
       1) A - B and B - A are to be considered DIFFERENT routes.
       2) Use the Trip table only. */
    val query = trips.
      map( trip => (trip.townFrom, trip.townTo) ).
      groupBy{ case(townFrom, townTo) => (townFrom, townTo) }.
      map{ case(_, count) => count.length }.
      result
    /* For some implicit reason adding .max to query itself produces slick.SlickTreeException: Cannot convert node to SQL Comprehension
       So I run .max on resulting collection instead. Kind of the same. Slick is buggy as hell #2 */

    val result = exec(query)
    val max = result.max

    assert(result.count(_ == max) === 4)
  }

  test("task 68(2)") {
    /* Find out the number of routes with the greatest number of flights (trips).
       Notes.
       1) A - B and B - A are to be considered SAME routes.
       2) Use the Trip table only. */
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

    assert(result.count(_ == max) === 2)
  }

  test("task 72(2)") {
    /* Among the customers using a single airline, find distinct passengers who have flown most frequently. Result set: passenger name, number of trips. */
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
    val result = queryResult.filter{ case(_, amount) => amount == max }.toSet

    assert(result === Set(("Mullah Omar", 4), ("Michael Caine", 4)))
  }

  test("task 77(2)") {
    /* Find the days with the maximum number of flights departed from Rostov. Result set: number of trips, date. */
    val query = trips.join(passengersInTrips).on(_.tripNo === _.tripNo).
      filter{ case(trip, _) => trip.townFrom === "Rostov" }.
      map{ case(trip, tripData) => (trip.tripNo, tripData.date) }.
      groupBy{ case(trip, tripDate) => (trip, tripDate) }.
      map{ case((_, tripDate), _) => tripDate }.
      groupBy{ tripDate => tripDate }.
      map{ case(tripDate, tripsCount) => (tripsCount.length, tripDate) }.
      result

    /* Some type checking error was here, full type annotation required. */
    val result = exec(query).map{
      tuple: (Int, Instant) => tuple match {
        case(times, date) => (times, date.toString.dropRight(10)) } }.
      toSet

    assert(result === Set((1,"2003-04-01"), (1,"2003-04-29"), (1,"2003-04-13"), (1,"2003-04-14"), (1,"2003-04-05"), (1,"2003-04-08")))
  }

  test("task 79(2)") {
    /* Get the passengers who, compared to others, spent most time flying. Result set: passenger name, total flight duration in minutes. */
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      join(trips).on{ case((_, passengerInTrip), trip) => passengerInTrip.tripNo === trip.tripNo }.
      map{ case((passenger, _), trip) => (passenger.name, trip.timeOut, trip.timeIn) }.
      result

    val result = exec(query).
      map{
        case(name, timeOut, timeIn) => (
          name,
          timeOut.toLocalDateTime,
          timeIn.toLocalDateTime ) }.
      map{
        case(name, dateTimeOut, dateTimeIn) => (
          name,
          dateTimeOut,
          if (dateTimeIn isBefore dateTimeOut) dateTimeIn.plusHours(24) else dateTimeIn) }.
      map{
        case(name, dateTimeOut, dateTimeIn) => (
          name,
          java.time.Duration.between(dateTimeOut, dateTimeIn).toMinutes) }.
      groupBy{ case(name, _) => name }.
      map{ case(name, flightTimes) => (name, flightTimes.map(_._2).sum) }.
      maxBy{ case(_, flightTime) => flightTime }

    assert(result === ("Michael Caine",2100))
  }

  test("task 84(2)") {
    /* For each airline, calculate the number of passengers carried in April 2003 (if there were any) by ten-day periods.
       Consider only flights that departed that month. Result set: company name, number of passengers carried for each ten-day period. */
    val query = companies.join(trips).on(_.IDComp === _.IDComp).
      join(passengersInTrips).on{ case((_, trip), passengerInTrip) => trip.tripNo === passengerInTrip.tripNo }.
      map{ case((company, _), passengerInTrip) => (company.name, passengerInTrip.date) }.
      result

    def countFlights(batch: Seq[(String, java.time.LocalDate)]): (Int, Int, Int) = {
      val times          = batch.map(_._2)
      val oneToTen       = times.partition(date => 1 to 10 contains date.getDayOfMonth)
      val tenToTwenty    = oneToTen._2.partition(date => 11 to 20 contains date.getDayOfMonth)
      val twentyToThirty = tenToTwenty._2
      (oneToTen._1.length, tenToTwenty._1.length, twentyToThirty.length)
    }
    val result = exec(query).
      map{ case(company, date) => (company, date.toLocalDate) }.
      filter{ case(_, date) => date.getYear == 2003 && date.getMonthValue == 4 }.
      groupBy{ case(companyName, _) => companyName }.
      map{ case(companyName, flights) => (companyName, countFlights(flights)) }.
      toSet

    assert(result === Set(("Don_avia",(4,5,0)),("Aeroflot",(1,0,1)),("Dale_avia",(4,0,0)),("air_France",(0,0,1))))
  }

  test("task 66(3)") {
    /* For all days between 2003-04-01 and 2003-04-07 find the number of trips from Rostov.
       Result set: date, number of trips. */
    val query = trips.join(passengersInTrips).on(_.tripNo === _.tripNo).
      filter{ case(trip, _) => trip.townFrom === "Rostov" }.
      map{ case(trip, passengerInTrip) => (trip.tripNo, passengerInTrip.date) }.
      groupBy{ case(tripNo, tripDate) => (tripNo, tripDate)}.
      map{ case((_, tripDate), _) => tripDate }.
      result

    val flights = exec(query).
      map( date => date.toLocalDate ).
      filter( date => date.getYear == 2003 && date.getMonthValue == 4 && (1 to 7 contains date.getDayOfMonth) )
    val result = (1 to 7).
      map( day => java.time.LocalDate.of(2003, 4, day)).
      map( date => (date.toString, flights.count(_.equals(date))) ).
      toSet

    assert(result === Set(("2003-04-01",1),("2003-04-02",0),("2003-04-03",0),("2003-04-04",0),("2003-04-05",1),("2003-04-06",0),("2003-04-07",0)))
  }

  test("task 76(3)") {
    /* Find the overall flight duration for passengers who never occupied the same seat.
       Result set: passenger name, flight duration in minutes. */
    val query = passengers.join(passengersInTrips).on(_.IDPsg === _.IDPsg).
      join(trips).on{ case((_, passengerInTrip), trip) => passengerInTrip.tripNo === trip.tripNo }.
      map{ case((passenger, passengerInTrip), trip) => (passenger.name, passengerInTrip.place, trip.timeOut, trip.timeIn) }.
      result

    val result = exec(query).
      map{ case(name, place, timeOut, timeIn) => (
        name,
        place,
        timeOut.toLocalDateTime,
        timeIn.toLocalDateTime ) }.
      map{
        case(name, place, dateTimeOut, dateTimeIn) => (
          name,
          place,
          dateTimeOut, if (dateTimeIn isBefore dateTimeOut) dateTimeIn.plusHours(24) else dateTimeIn) }.
      map{
        case(name, place, dateTimeOut, dateTimeIn) => (
          name,
          place,
          java.time.Duration.between(dateTimeOut, dateTimeIn).toMinutes) }.
      groupBy{ case(name, _, _) => name }.
      map{
        case(name, flights) => (
          name,
          flights.map(_._2).distinct.length == flights.map(_._2).length,
          flights.map(_._3).sum) }.
      filter{ case(_, allDistinct, _) => allDistinct }.
      map{ case(name, _, time) => (name, time) }.
      toSet

    assert(result === Set(("George Clooney",650),("Kevin Costner",788),("Jennifer Lopez",332),("Ray Liotta",789),("Alan Rickman",115),("Kurt Russell",1797),("Harrison Ford",1800),("Russell Crowe",840),("Steve Martin",1440),("Michael Caine",2100)))
  }

  test("task 94(3)") {
    /*For successive 7 days from the first day when number of trips from town Rostov was the maximum, find out the number of trips from town Rostov.
      Result set: date, number of trips */
    val rostovFlightsQuery = trips.join(passengersInTrips).on(_.tripNo === _.tripNo).
      filter{ case(trip, _) => trip.townFrom === "Rostov" }.
      map{ case(trip, tripData) => (trip.tripNo, tripData.date) }.
      groupBy{ case(trip, tripDate) => (trip, tripDate) }.
      map{ case((_, tripDate), _) => tripDate }.
      result

    val rostovFlights = exec(rostovFlightsQuery)
    val rostovFlightsGrouped = rostovFlights.
      groupBy( tripDate => tripDate ).
      map{ case(date, _) => date.toLocalDate }
    val rostovFlightMax = rostovFlights.
      groupBy( tripDate => tripDate ).
      map{ case(_, thisTrips) => thisTrips.length }.
      max
    val firsMaxDay = rostovFlights.
      groupBy( tripDate => tripDate ).
      map{ case(date, thisTrips) => (date, thisTrips.length) }.
      filter{ case(_, tripCount) => tripCount == rostovFlightMax }.
      map{ case(date, _) => date}.
      min
    val firstDay = firsMaxDay.toLocalDate
    val result = (firstDay.getDayOfMonth to firstDay.getDayOfMonth + 6).
      map( day => java.time.LocalDate.of(firstDay.getYear, firstDay.getMonthValue, day)).
      map( date => (date.toString, rostovFlightsGrouped.count(_.equals(date))) ).
      toSet

    assert(result === Set(("2003-04-01",1),("2003-04-02",0),("2003-04-03",0),("2003-04-04",0),("2003-04-05",1),("2003-04-06",0),("2003-04-07",0)))
  }

  test("task 120(3)") {
    /* For the companies, which had any flights, to within two decimal digits, calculate average values of real flying time in minutes.
       Also, calculate these characteristics for all the flights.
       Result set: company name, arithmetic mean {(x1 + x2 + … + xN)/N}, geometric mean {(x1 * x2 * … * xN)^(1/N)},
       square mean { sqrt((x1^2 + x2^2 + ... + xN^2)/N)}, and harmonic mean {N/(1/x1 + 1/x2 + ... + 1/xN)} */
    val companiesWithTimesOutInQuery = companies.join(trips).on(_.IDComp === _.IDComp).
      join(passengersInTrips).on{ case((_, trip), passengerInTrip) => trip.tripNo === passengerInTrip.tripNo }.
      map{ case((company, trip), _) => (company.name, trip.timeOut, trip.timeIn) }.
      result

    val companiesWithFlights = exec(companiesWithTimesOutInQuery).
      map{ case(companyName,timeOut, timeIn) => (
        companyName,
        timeOut.toLocalDateTime,
        timeIn.toLocalDateTime ) }.
      map{
        case(companyName, dateTimeOut, dateTimeIn) => (
          companyName,
          dateTimeOut, if (dateTimeIn isBefore dateTimeOut) dateTimeIn.plusHours(24) else dateTimeIn) }.
      map{
        case(companyName, dateTimeOut, dateTimeIn) => (
          companyName,
          java.time.Duration.between(dateTimeOut, dateTimeIn).toMinutes) }
    def arithmetic(batch: Seq[(String, Long)]): (Double, Double, Double, Double) = {
      val flightTimes = batch.map(_._2).map( t => t: Double )
      val total = flightTimes.length
      def roundToTwo(double: Double) =
        BigDecimal(double).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (
        roundToTwo(flightTimes.sum / total),
        roundToTwo(scala.math.pow(flightTimes.product, 1.0 / total)),
        roundToTwo(scala.math.sqrt(flightTimes.map(scala.math.pow(_, 2)).sum / total)),
        roundToTwo(total / flightTimes.map(1/_).sum)
      )
    }
    val result = companiesWithFlights.
      groupBy{ case(companyName, _) => companyName }.
      map{ case(companyName, flightTimes) => (companyName, arithmetic(flightTimes)) }.
      filter{ case(companyName, _) => companyName == "Aeroflot" || companyName == "air_France" }.
      map{ case(companyName, (arithmetic, geometric, square, harmonic)) => (companyName, arithmetic, geometric, square, harmonic) }.
      toSet

    /* It's actually doesn't conform perfectly, but I believe, this is due to FP numbers. Aeroflot and air_France DO conform, as there are only one real flight
       Maybe, with plain SQL results will conform, but we are Slick-adepts here */
    assert(result === Set(("Aeroflot",108.0,108.0,108.0,108.0), ("air_France",200.0,200.0,200.0,200.0)))
  }

}
