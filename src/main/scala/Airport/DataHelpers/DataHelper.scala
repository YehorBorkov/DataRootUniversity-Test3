package Airport.DataHelpers

trait DataHelper[T] {
  def getData(resource: String): Seq[T]
}
