object binary_integer {

  def int_binary(number: Int): String = {
    var num = number;
    if(num==0) "0"
    var binary_string = ""
    while(num!=0){
      binary_string = num%2+binary_string
      num/=2
    }
    binary_string
  }

  def binary_int(number: String): Int = {
    var num = number;
    if(num=="0") 0
    var integer = 0
    var power = 0
    while(num.length()!=0){
      integer = math.pow(2*(num.last.toInt%2), power).toInt+integer
      power+=1
      num = num.dropRight(1)
    }
    integer
  }

  def main(args: Array[String]) {

    var number = 53
    println(number.toBinaryString)
    println(int_binary(number))
    println(binary_int("110101"))
  }
}
