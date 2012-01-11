package algorithms

import scala.annotation.tailrec
import scala.collection.immutable._
import scala.collection.immutable.Stream._
import scala.util.Random

/**
 * Implementation of the Luby Transform Fountain Code.
 *
 * The algorithm is as follows for each block in the input file
 *
 * 1. Choose a psuedo-random number, d, between 1 and k (number of total blocks in file/stream)
 * 2. Choose d blocks psuedo-randomly (seeded with d) from the file and combine them using xor
 * 3. Transmit encoded blocks along with seed for the psuedo-random number
 *
 * Note that d should follow the Robust Soliton Distribution for optimal transmission size
 */
class LubyTransform (val seed: Int = Random.nextInt(), val blockSize: Int = 32768) {
    import LubyTransform._

    /**
     * Transforms a stream of data into a stream of encoded blocks using an RSD distribution function.
     */
    def transform (data: Seq[Array[Byte]]): Stream[Block] = transform(data, distribution(seed, data.length))

    /**
     * Transforms a stream of data into a stream of encoded blocks using the provided distribution function.
     */
    private def transform (input: Seq[Array[Byte]], degrees: Stream[Int]): Stream[Block] = input match {
        case Empty => Empty
        case _ =>
            val blocks = select(input, degrees.head, distribution(degrees.head, input.length - 1))
            combine(blocks) match {
                case None => Empty
                case Some(xor) => cons(Block(degrees.head, false, xor), transform(input, degrees.tail))
            }
    }
}

object LubyTransform {

    /**
     * Stream of random numbers from 0 (inclusive) to bound (exclusive), used as a distribution.
     */
    def distribution (seed: Int, bound: Int): Stream[Int] = {
        val random = new Random(seed)
        def distribution (random: Random): Stream[Int] = cons(random.nextInt(bound + 1), distribution(random))
        distribution(random)
    }

    /**
     * Nondestructively combines an arbitrary number of equal length byte arrays by XOR
     */
    def combine (data: Seq[Array[Byte]]): Option[Array[Byte]] = data match {
        case Nil => None
        case x::xs => combine(xs) match {
            case None => Some(x)
            case Some(x2) => Some(xor(x, x2))
        }
    }

    /**
     * Selects a unique psuedo-random set of blocks from the input data. Chooses via random index pulled from a stream
     * and discards duplicates; for this reason the count must not be higher than the range of the random number
     * generator.
     */
    def select (input: Seq[Array[Byte]], count: Int, random: Stream[Int], used: Set[Int] = Set()): List[Array[Byte]] = count match {
        case 0 => Nil
        case _ =>
            val next = random.filter(!used.contains(_)).head
            input(next) :: select(input, count - 1, random.tail, used + next)
    }

    /**
     * Combines two byte arrays by XOR nondestructively
     */
    def xor (one: Array[Byte], two: Array[Byte]) = {
        val (larger, smaller) = sort(one, two)

        val result = larger.clone
        for (i <- 0 until smaller.length) result.update(i, (larger(i) ^ smaller(i)).asInstanceOf[Byte])
        result
    }

    /**
     * Sort the arrays by size
     */
    def sort (one: Array[Byte], two: Array[Byte]) = if (one.length >= two.length) (one, two) else (two, one)
}

case class Block (val seed: Int, val last: Boolean, val data: Array[Byte])
