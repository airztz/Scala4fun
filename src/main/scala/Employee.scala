class Employee (var name: String, var salary: Int){
  var dept = 0

  def printEmployee(): Unit ={
    println(name+" makes $"+salary)
  }
}
class TestFunction{
  def m(x: Int) = x + 1      //def defines a method
  def sumSalary(eList:List[Employee])={
    var sum = 0
    //eList.foreach((e: Employee) => sum+=e.salary)
    //eList.foreach(e => sum+=e.salary)
    eList.foreach(e => {
      if(e.dept == 10) sum+=e.salary
    })
    sum
  }
  def sumSalaryV2(eList: List[Employee], udf_selection: Employee => Boolean)={
    var sum = 0
    eList.foreach(e => {
      if(udf_selection(e)) sum+=e.salary
    })
    sum
  }
  //Let's define some functions
  val f = (x: Int) => x + 1  //val f defines a function, and scala will create another new class under the hood, cuz scala treats f as a class
  val simpleStringFun = () => "Hi There!"
  val evenOrOdd = (x: Int) => {
    if(x % 2 == 0) "even"
    else "odd"
  }
  //
}
object Main{
  //Unit is like void in Java
  def main(args: Array[String]): Unit ={
//    var emily = new Employee("Emily", 10000)
//    println(emily.name)
//    println(emily.salary)
//    emily.salary = 20000
//    println(emily.salary)
//    println(emily.dept)
//    emily.dept = 10   //set it to "" will raise a compile time error
//    emily.printEmployee()

    val t = new TestFunction()
    println(t.m(10))
    println(t.f(10))
    println(t.simpleStringFun())
    println(t.evenOrOdd(11))

    var emily = new Employee("Emily",1000)
    emily.dept = 10
    var john = new Employee("John", 2000)
    john.dept = 11
    var sam = new Employee("Sam", 10000)
    sam.dept = 10
    var riley = new Employee("Riley", 3000)
    riley.dept = 13
    var jim = new Employee("Jim", 6000)
    jim.dept = 10

    val elist = List(emily, john, sam, riley, jim)

    println("Total salary: " + t.sumSalary(elist))

    //Here we define a function called udf
    //We can define it in three fashions:

    //val udf: Employee => Boolean = (e: Employee)=>e.dept==10
    val udf : Employee => Boolean = e=>e.dept==10
    //val udf = (e: Employee)=>e.dept==10
    println("Total salary: " + t.sumSalaryV2(elist,udf))

    //Or
    //println("Total salary: " + t.sumSalaryV2(elist,(e: Employee)=>e.dept==10))
  }
}
