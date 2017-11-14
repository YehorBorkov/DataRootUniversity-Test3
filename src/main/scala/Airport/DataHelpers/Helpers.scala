package Airport.DataHelpers

import scala.io.Source
import Airport.DBTables._

import scala.util.matching.Regex

object CompanyDataHelper extends DataHelper[Company] {
  val companyData: Regex = """\((\d+),\"(\D+)\"\)""".r
  def getData(resource: String): Seq[Company] =
    Source.fromResource(resource).
      getLines.
      map {case companyData(comp_id, name) => Company(Some(comp_id.toInt), name)}.
      toSeq
}

object PassengerInTripDataHelper extends DataHelper[PassengerInTrip] {
  val passengerInTripData: Regex = """\((\d+),\"(.+)\",(\d+),\"(.+)\"\)""".r
  def getData(resource: String): Seq[PassengerInTrip] =
    Source.fromResource(resource).
      getLines.
      map {case passengerInTripData(tripId, dateString, passengerId, place) => PassengerInTrip(tripId.toInt, dateString, passengerId.toInt, place)}.
      toSeq
}

object PassengerDataHelper extends DataHelper[Passenger] {
  val passengerData: Regex = """\((\d+),\"(\D+)\"\)""".r
  def getData(resource: String): Seq[Passenger] =
    Source.fromResource(resource).
      getLines.
      map {case passengerData(pass_id, name) => Passenger(Some(pass_id.toInt), name)}.
      toSeq
}

object TripDataHelper extends DataHelper[Trip] {
  val tripData: Regex = """\((\d+),(\d+),\"(.+)\",\"(\D+)\",\"(\D+)\",\"(.+)\",\"(.+)\"\)""".r
  def getData(resource: String): Seq[Trip] =
    Source.fromResource(resource).
      getLines.
      map {case tripData(tripId, compId, plane, from, to, timeOut, timeIn) => Trip(Some(tripId.toInt), compId.toInt, plane, from, to, timeOut, timeIn)}.
      toSeq
}
