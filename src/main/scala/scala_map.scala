object scala_map {
  def main(args: Array[String]) {
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")

    println("capitals.get( \"France\" ) : " +  capitals.get( "France" ))
    println("capitals.get( \"India\" ) : " +  capitals.get( "India" ))
  }
}
