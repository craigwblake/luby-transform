package algorithms

import scala.annotation.tailrec
import scala.collection.immutable.Stream._
import scala.util.Random

class LubyTransform (val seed: Int = Random.nextInt()) {
    import LubyTransform._

    val random = new Random(seed)

    /**
     * Stream of random number used as a distribution for encoding.
     */
    val distribution: Stream[Int] = cons(random.nextInt(5) + 1, distribution)

    /**
     * Transforms a stream of input data into a stream of encoded blocks using the seeded distribution function.
     */
    def transform (data: Stream[Array[Byte]]): Stream[Block] = transformWithDistribution(data, distribution)

    /**
     * Transforms a stream of input data into a stream of encoded blocks using the provided distribution function.
     */
    private def transformWithDistribution (input: Stream[Array[Byte]], ints: Stream[Int]): Stream[Block] = input match {
        case Empty => Empty
        case _ =>
            val (xor, remainder) = combine(input.head, input.tail, ints.head)
            cons(Block(seed, xor), transformWithDistribution(remainder, ints.tail))
    }
}

object LubyTransform {

    /**
     * Combines an arbitrary number of equal length byte arrays by XOR
     */
    @tailrec
    def combine (head: Array[Byte], tail: Stream[Array[Byte]], remaining: Int): (Array[Byte], Stream[Array[Byte]]) = remaining match {
        case 0 => (head, tail)
        case _ if tail == Empty => (head, tail)
        case _ => combine(xor(head, tail.head), tail.tail, remaining - 1)
    }

    /**
     * Combines two byte arrays by XOR
     */
    def xor (one: Array[Byte], two: Array[Byte]) = {
        for (i <- 0 until two.length) one.update(i, (one(i) ^ two(i)).asInstanceOf[Byte])
        one
    }
}

case class Block (val seed: Int, val data: Array[Byte])
