package algorithms

import java.io.{File,FileInputStream,RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode._
import Int._
import scala.annotation.tailrec
import scala.collection._
import scala.collection.immutable.Stream._
import scala.collection.immutable.TreeSet
import scala.util.Random

case class Block (val seed: Int, val size: Int, val chunk: Int, val data: Array[Byte])

case class PreparedBlock (val chunks: SortedSet[Int], val data: Array[Byte])

/**
 * Implementation of a Luby Transform rateless error correcting code (fountain code).
 *
 * The algorithm is as follows for each block in the input file
 *
 * 1. Choose a psuedo-random number, d, between 1 and k (number of total blocks in file/stream)
 * 2. Choose d blocks psuedo-randomly (seeded with d) from the file and combine them using xor
 * 3. Transmit encoded blocks along with seed for the psuedo-random number
 *
 * Note that d should follow the Robust Soliton Distribution for near optimal transmission size
 */
class LubyTransform (val data: ByteBuffer, val seed: Int = Random.nextInt(), val chunk: Int = 1024) {
    import LubyTransform._

    val chunks = ByteBufferSeq(data, chunk)

    /**
     * Transforms a stream of data into a stream of encoded blocks using an RSD distribution function.
     */
    def transform: Stream[Block] = transform(distribution(seed))

    /**
     * Transforms a stream of data into a stream of encoded blocks using the provided distribution function.
     */
    private def transform (seeds: Stream[Int]): Stream[Block] = chunks.length match {
        case 0 => Empty
        case _ =>
            val degrees = distribution(seeds.head, chunks.length)
            val selection = select(degrees.head + 1, distribution(degrees.tail.head, chunks.length))
            val blocks = selection.map(x => chunks(x))

            combine(blocks.toSeq) match {
                case None => Empty
                case Some(xor) => cons(Block(seeds.head, data.capacity, chunk, xor), transform(seeds.tail))
            }
    }
}

object LubyTransform {

    implicit def file2buffer (file: File): ByteBuffer = new FileInputStream(file).getChannel.map(READ_ONLY, 0, file.length)

    def read (source: Stream[Block], destination: File): Unit = {
        val mapping = new RandomAccessFile(destination, "rw").getChannel.map(READ_WRITE, 0, source.head.size)
        val buffer = ByteBufferSeq(mapping, source.head.chunk)
        decode(buffer, source)
    }

    def decode (destination: ByteBufferSeq, source: Stream[Block], decoded: Set[Int] = TreeSet[Int](), blocks: Set[PreparedBlock] = Set(), read: Int = 0): Unit = read match {
        case read if read < destination.length =>

            val block = source.head
            
            val degrees = distribution(block.seed, destination.length)
            val degree = degrees.head + 1
            
            // this is repeating the same blocks for each degree
            val selection = select(degree, distribution(degrees.tail.head, destination.length))

            val partition = selection.partition(x => decoded.contains(x))
            (partition._1.toSeq, partition._2.toSeq) match {
                
                case (_, Seq()) => // Already decoded, throw it away
                    decode(destination, source.tail, decoded, blocks, read)
                  
                case (ready, Seq(x)) => // One left, we can decode!
                    val data = ready.map(x => destination(x)) match {
                        case Nil => Some(block.data)
                        case chunks => combine(chunks :+ block.data)
                    }
                    destination.update(x, data.get)
                    //retry(blocks, decoded)
                    decode(destination, source.tail, decoded + x, blocks, read + 1)
                
                case (_, set) => // Too many unknowns
                    decode(destination, source.tail, decoded, blocks + PreparedBlock(TreeSet[Int]() ++ set, block.data), read)
            }

        case _ => // Finished!
    }

    def calculateChunkCount (size: Int, chunk: Int): Int = (size + chunk - 1) / chunk

    /**
     * Stream of random numbers from 0 (inclusive) to bound (exclusive), used as a distribution.
     */
    def distribution (seed: Int, bound: Int = MaxValue): Stream[Int] = {
        val random = new Random(seed)
        def distribution (r: Random): Stream[Int] = cons(random.nextInt(bound), distribution(r))
        distribution(random)
    }

    /**
     * Nondestructively combines an arbitrary number of equal length byte arrays with an XOR operation
     */
    def combine (data: Seq[Array[Byte]]): Option[Array[Byte]] = data match {
        case Seq() => None
        case Seq(x, xs@_*) => combine(xs) match {
            case None => Some(x)
            case Some(x2) => Some(xor(x, x2))
        }
    }

    /**
     * Selects a unique psuedo-random set of ints from the given stream. This function discards duplicates;
     * for this reason the count must not be higher than the range of the random number generator.
     */
    def select (count: Int, random: Stream[Int], used: Set[Int] = Set()): SortedSet[Int] = count match {
        case 0 => TreeSet()
        case _ =>
            val next = random.filter(!used.contains(_)).head
            select(count - 1, random.tail, used + next) + next
    }

    /**
     * Nondestructively combines two arbitrary-length byte arrays with an XOR operation
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

case class ByteBufferSeq (buffer: ByteBuffer, chunk: Int) extends mutable.IndexedSeq[Array[Byte]] {
    import LubyTransform._

    implicit def int2bigdecimal (v: Int) = BigDecimal(v)
    
    override val length = calculateChunkCount(buffer.capacity, chunk)

    override def update (index: Int, elem: Array[Byte]) = {
        val start = index * chunk
        val end = Math.min(start + elem.length, buffer.capacity)

        buffer.limit(end)
        buffer.position(start)
        buffer.put(elem, 0, end - start)
    }

    override def apply (index: Int): Array[Byte] = {
        val start = index * chunk
        val end = Math.min(start + chunk, buffer.capacity)
        val array = new Array[Byte](end - start)

        buffer.limit(end)
        buffer.position(start)
        buffer.get(array, 0, end - start)
        array
    }
}
