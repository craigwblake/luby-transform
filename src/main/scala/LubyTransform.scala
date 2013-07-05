package algorithms

import java.io.{File,FileInputStream,RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode._
import Int._
import scala.annotation.tailrec
import scala.collection._
import scala.collection.immutable.Stream._
import scala.collection.mutable.IndexedSeq
import scala.util.Random

case class Block (val seed: Int, val size: Int, val chunk: Int, val data: Array[Byte])

case class PreparedBlock (val chunks: List[Int], val data: Array[Byte])

/**
 * Implementation of a Luby Transform fountain code.
 *
 * The algorithm is as follows for each block in the input file
 *
 * 1. Choose a psuedo-random number, d, between 1 and k (number of total blocks in file)
 * 2. Choose d blocks psuedo-randomly (seeded with d) from the file and xor them
 * 3. Transmit encoded blocks along with seed for the psuedo-random number
 *
 * Note that d should follow the Robust Soliton Distribution for optimal transmission size
 */
class LubyTransform (val data: ByteBuffer, val seed: Int = Random.nextInt(), val chunk: Int = 2048) {
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

    /**
     * Writes a data block stream into the given file and returns the total count of read chunks.
     */
    def write (source: Stream[Block], destination: File): Long = {
        val mapping = new RandomAccessFile(destination, "rw").getChannel.map(READ_WRITE, 0, source.head.size)
        val buffer = ByteBufferSeq(mapping, source.head.chunk)
        decode(buffer, source)
    }

    /**
     * Writes a data block stream into the given buffer.
     */
    @tailrec
    def decode (destination: ByteBufferSeq, source: Stream[Block], available: List[Int] = List[Int](), waiting: List[PreparedBlock] = List(), iterations: Long = 0L): Long = available.size match {
        case read if read < destination.length =>
            val block = source.head
            val degrees = distribution(block.seed, destination.length)
            val degree = degrees.head + 1
            val selection = select(degree, distribution(degrees.tail.head, destination.length))
            val partition = selection.partition(x => available.contains(x))

            (partition._1, partition._2) match {
                case (_, Nil) => // Already available, throw it away
                    decode(destination, source.tail, available, waiting, iterations + 1)
                  
                case (ready, x :: Nil) => // One left, we can decode!
                    decodeChunk(destination, PreparedBlock(selection, block.data), available)
                    val result = decodeSaved(destination, x :: available, waiting)
                    decode(destination, source.tail, result._1, result._2, iterations + 1)
                
                case (ready, unready) => // Too many unknowns
                    decode(destination, source.tail, available, PreparedBlock(ready ++ unready, block.data) :: waiting, iterations + 1)
            }

        case _ => iterations // Finished!
    }

    /**
     * Finds and decodes deferred blocks, recursing the list again each time a new block is decoded.
     */
    @tailrec
    def decodeSaved (destination: ByteBufferSeq, available: List[Int], unchecked: List[PreparedBlock]): (List[Int], List[PreparedBlock]) = decodeSavedPass(destination, available, unchecked) match {
        case (available, waiting, true) => (available, waiting)
        case (available, waiting, false) => decodeSaved(destination, available, waiting)
    }

    /**
     * A single pass through the deferred blocks to decode any that are now available.
     */
    @tailrec
    def decodeSavedPass (destination: ByteBufferSeq, available: List[Int], unchecked: List[PreparedBlock], checked: List[PreparedBlock] = List()): (List[Int], List[PreparedBlock], Boolean) = unchecked.size match {
        case 0 => (available, checked, true)
        case _ =>
            val block = unchecked.head
            decodeChunk(destination, block, available) match {
                case Some(index) => (index :: available, checked ++ unchecked.tail, false)
                case None => decodeSavedPass(destination, available, unchecked.tail, block :: checked)
            }
    }

    /**
     * Decodes a single chunk into the destination.
     */
    def decodeChunk (destination: IndexedSeq[Array[Byte]], block: PreparedBlock, available: List[Int]): Option[Int] = {
        val intersection = block.chunks.intersect(available)
        val remaining = block.chunks filterNot (intersection contains)

        remaining.size match {
            case 1 =>
                val data = intersection.map(destination(_)).toSeq match {
                    case Nil => Some(block.data)
                    case chunks => combine(chunks :+ block.data)
                }
                destination.update(remaining.head, data.get)
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

    val Modulo = 87178291199L // prime
    val Incrementor = 17180131327L // relative prime

    def select2 (count: Int, random: Stream[Int]): List[Long] = select2(count, random.head, Nil)

    // Implementation of non-repeating linear congruential pseudo-random generator
    // TODO: Convert to stream to save memory
    @tailrec
    def select2 (count: Int, seed: Long, list: List[Long]): List[Long] = count match {
        case 0 => list
        case _ =>
            val next = (seed + Incrementor) % Modulo
            select2(count - 1, next, next :: list)
    }

    /**
     * Selects a unique psuedo-random set of ints from the given stream. This function discards duplicates;
     * for this reason the count must not be higher than the range of the random number generator.
     */
    //@tailrec
    def select (count: Int, random: Stream[Int], used: List[Int] = List()): List[Int] = count match {
        case 0 => Nil
        case _ =>
            //val next = random.filter(!used.contains(_)).head
            val next = random.head
            next :: select(count - 1, random.tail, next :: used)
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

/**
 * Sequence view of a byte buffer in chunks.
 */
case class ByteBufferSeq (buffer: ByteBuffer, chunk: Int) extends IndexedSeq[Array[Byte]] {
    import LubyTransform._

    implicit def int2bigdecimal (v: Int) = BigDecimal(v)
    
    /**
     * Calculates the number of chunks in this view.
     */
    override val length = calculateChunkCount(buffer.capacity, chunk)

    /**
     * Writes data to the chunk at the given index.
     */
    override def update (index: Int, elem: Array[Byte]) = {
        val start = index * chunk
        val end = Math.min(start + elem.length, buffer.capacity)

        buffer.limit(end)
        buffer.position(start)
        buffer.put(elem, 0, end - start)
    }

    /**
     * Reads data from the chunk at the given index.
     */
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
