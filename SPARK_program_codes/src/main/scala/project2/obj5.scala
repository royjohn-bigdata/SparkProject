package project2

import scala.util.control._


object Mathfct {

	def add (x:Int,y:Int): Int ={
			return x+y
	}

	def sub (x:Int,y:Int):Int={
			return x-y;
	}

	def mul (x:Int,y:Int):Int={
			return x*y
	}

	def div (x:Double,y:Double):Double={
			return x/y
	}

	def square(x:Int):Int={
			return x*x
	}
}



object obj5 {
	def main (args:Array[String]): Unit={
			    var loop = new Breaks
					var d=0
					loop.breakable
					  {
        				do
        				{
         					println("Mathematics Operation")
        					println("\n1. Addition\n")
        					println("2. Substraction\n")
        					println("3. Multiplication\n")
        					println("4. Square\n")
        					println("5. Exit\n")
        
        					println("Enter your choice :")
        					d = scala.io.StdIn.readInt()
        					d match
        					{
           					case 1 =>
          					println("Enter number 1:")
          					var n1 = scala.io.StdIn.readInt()
          					println("Enter number 2:")
          					var n2 = scala.io.StdIn.readInt()
          					println("Sum of two numbers is :" + Mathfct.add(n1, n2) )
          					case 2 =>
          					println("Enter number 1:")
          					var n1 = scala.io.StdIn.readInt()
          					println("Enter number 2:")
          					var n2 = scala.io.StdIn.readInt()
          					println("Substraction of two numbers is :"  + Mathfct.sub(n1, n2)  )
          					case 3 => 
          					println("Enter number 1:")
          					var n1 = scala.io.StdIn.readInt()
          					println("Enter number 2:")
          					var n2 = scala.io.StdIn.readInt()
          					println("Multiplication of two numbers is :"  + Mathfct.mul(n1, n2)  )
          					case 4 => 
          					println("Enter number 1:")
          					var n1 = scala.io.StdIn.readInt()
          					println("Square of the number is :"  + Mathfct.square(n1)  )
          					case 5 => loop.break()
        					}
        				} while(d >=1 || d<=5)
      			}

			  println("==========Operations Completed! Thank You=======")
	}
}