package algorithms

import java.io.{File,FileInputStream,RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode._
import Int._
import scala.annotation.tailrec
import scala.collection._
import scala.collection.immutable.Stream._
import scala.collection.immutable.TreeSet
import scala.collection.mutable.IndexedSeq
import scala.util.Random

case class Block (val seed: Int, val size: Int, val chunk: Int, val data: Array[Byte])

case class PreparedBlock (val chunks: Set[Int], val data: Array[Byte])

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
            //for (i <- selection) println("encoded chunk " + i + ": " + new String(chunks(i)))

            combine(blocks.toSeq) match {
                case None => Empty
                case Some(xor) => cons(Block(seeds.head, data.capacity, chunk, xor), transform(seeds.tail))
            }
    }
}

object LubyTransform {

    implicit def file2buffer (file: File): ByteBuffer = new FileInputStream(file).getChannel.map(READ_ONLY, 0, file.length)

    implicit object O extends Ordering[Int] { def compare(x: Int, y: Int): Int = x - y }

    def read (source: Stream[Block], destination: File): Unit = {
        val mapping = new RandomAccessFile(destination, "rw").getChannel.map(READ_WRITE, 0, source.head.size)
        val buffer = ByteBufferSeq(mapping, source.head.chunk)
        decode(buffer, source)
    }

    @tailrec
    def decode (destination: ByteBufferSeq, source: Stream[Block], available: Set[Int] = Set[Int](), waiting: Set[PreparedBlock] = Set(), read: Int = 0): Unit = available.size match {
        case read if read < destination.length =>
            val block = source.head
            val degrees = distribution(block.seed, destination.length)
            val degree = degrees.head + 1
            val selection = select(degree, distribution(degrees.tail.head, destination.length))
            val partition = selection.partition(x => available.contains(x))

            //println("read " + read + ", available " + (TreeSet[Int]() ++ available).mkString(","))
            (partition._1.toSeq, partition._2.toSeq) match {
                case (_, Seq()) => // Already available, throw it away
                    //println("skipping chunks " + selection.mkString(",") + " - already available")
                    decode(destination, source.tail, available, waiting, read)
                  
                case (ready, Seq(x)) => // One left, we can decode!
                    decodeChunk(destination, PreparedBlock(selection, block.data), available)
                    val result = decodeSaved(destination, available + x, waiting)
                    decode(destination, source.tail, result._1, result._2, read + 1)
                
                case (_, seq) => // Too many unknowns
                    //println("not ready to decode chunks " + selection.mkString(","))
                    decode(destination, source.tail, available, waiting + PreparedBlock(seq.toSet, block.data), read)
            }

        case _ => // Finished!
    }

    @tailrec
    def decodeSaved (destination: ByteBufferSeq, available: Set[Int], unchecked: Set[PreparedBlock]): (Set[Int], Set[PreparedBlock]) = decodeSavedPass(destination, available, unchecked) match {
        case (available, waiting, true) => (available, waiting)
        case (available, waiting, false) => decodeSaved(destination, available, waiting)
    }

    @tailrec
    def decodeSavedPass (destination: ByteBufferSeq, available: Set[Int], unchecked: Set[PreparedBlock], checked: Set[PreparedBlock] = Set()): (Set[Int], Set[PreparedBlock], Boolean) = unchecked.size match {
        case 0 => (available, checked, true)
        case _ =>
            val block = unchecked.head
            decodeChunk(destination, block, available) match {
                case Some(index) => (available + index, checked ++ unchecked.tail, false)
                case None => decodeSavedPass(destination, available, unchecked.tail, checked + block)
            }
    }

    def decodeChunk (destination: IndexedSeq[Array[Byte]], block: PreparedBlock, available: Set[Int]): Option[Int] = {
        val intersection = block.chunks.intersect(available)
        val remaining = block.chunks -- intersection
        //println("intersection: " + intersection.mkString(","))
        //println("remaining: " + remaining.mkString(","))

        remaining.size match {
            case 1 =>
                val data = intersection.map(destination(_)).toSeq match {
                    case Nil => Some(block.data)
                    case chunks =>
                        combine(chunks :+ block.data)
                }
                println("decoding chunks " + remaining.head + ": " + new String(data.get))
                if (remaining.head == 7) {
                    println("\nfound 7")
                    println("other parts: " + intersection.mkString(","))
                    val chunk14 = destination(14)
                    println("bytes for chunk # 14: 	\\" + chunk14(0).toInt + " 	\\" + chunk14(1).toInt + " 	\\" + chunk14(2).toInt + " 	\\" + chunk14(3).toInt + " 	\\" + chunk14(4).toInt + ": " + new String(chunk14))
                    val chunk19 = destination(19)
                    println("bytes for chunk # 19: 	\\" + chunk19(0).toInt + " 	\\" + chunk19(1).toInt + " 	\\" + chunk19(2).toInt + " 	\\" + chunk19(3).toInt + " 	\\" + chunk19(4).toInt + ": " + new String(chunk19))
                    println("bytes for this chunk: 	\\" + block.data(0).toInt + " 	\\" + block.data(1).toInt + " 	\\" + block.data(2).toInt + " 	\\" + block.data(3).toInt + " 	\\" + block.data(4).toInt)
                    println("processed this chunk: 	\\" + data.get(0).toInt + " 	\\" + data.get(1).toInt + " 	\\" + data.get(2).toInt + " 	\\" + data.get(3).toInt + " 	\\" + data.get(4).toInt)
                    println("\n")
                }
                destination.update(remaining.head, data.get)
                println("line is: " + destination.map(x => new String(x)).mkString.trim)
                Some(remaining.head)
            case _ => None
        }
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
            case Some(intermediate) => Some(xor(intermediate, x))
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

case class ByteBufferSeq (buffer: ByteBuffer, chunk: Int) extends IndexedSeq[Array[Byte]] {
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
