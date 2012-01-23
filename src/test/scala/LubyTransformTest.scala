package algorithms

import com.github.tototoshi.base64.Base64.encode
import java.io.File
import java.nio.ByteBuffer._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.annotation.tailrec
import scala.collection.immutable.Stream._
import scala.collection.mutable.IndexedSeq
import scala.collection._
import scala.io.Source

class LubyTransformSpec extends FlatSpec with ShouldMatchers {

    implicit def string2bytes (v: String) = v.getBytes("ASCII")

    "A byte buffer sequence" should "calculate the number of chunks in a buffer" in {
        val buffer = allocate(4096)
        ByteBufferSeq(buffer, 1024).size should equal (4)
        ByteBufferSeq(buffer, 1000).size should equal (5)
    }

    it should "iterate all chunks in the buffer" in {
        val buffer = allocate(4)
        buffer.put(0, 'a')
        buffer.put(1, 'b')
        buffer.put(2, 'c')
        buffer.put(3, 'd')

        val iterator = ByteBufferSeq(buffer, 1).iterator
        iterator.hasNext should equal (true)
        iterator.next()(0) should equal ('a')
        iterator.hasNext should equal (true)
        iterator.next()(0) should equal ('b')
        iterator.hasNext should equal (true)
        iterator.next()(0) should equal ('c')
        iterator.hasNext should equal (true)
        iterator.next()(0) should equal ('d')
        iterator.hasNext should equal (false)
    }

    it should "iterate variable length buffers" in {
        val buffer = allocate(1024)
        val iterator = ByteBufferSeq(buffer, 500).iterator
        iterator.next.length should equal (500)
        iterator.next.length should equal (500)
        iterator.next.length should equal (24)
    }

    "A distribution" should "return pseudo-random integers between 0 (inclusive) and bound (exclusive)" in {
        val distribution = LubyTransform.distribution(1, 5)
        distribution.head should equal (0)
        distribution.tail.head should equal (3)
        distribution.tail.tail.head should equal (2)

        @tailrec
        def testBounds (distribution: Stream[Int], remaining: Int = 1000): Unit = remaining match {
            case 0 =>
            case _ =>
                distribution.head should be >= (0)
                distribution.head should be < (5)
                testBounds(distribution.tail, remaining - 1)
        }
        testBounds(distribution)
    }

    it should "return the entire set of possibilities over time" in {
        val distribution = LubyTransform.distribution(1, 20)
        val numerals = 0 until 20
        def waitForAll (distribution: Stream[Int], numerals: List[Int]): Unit = numerals match {
            case Nil =>
            case x => waitForAll(distribution.tail, numerals - distribution.head)
        }
        waitForAll(distribution, numerals.toList)
    }

	"A Luby Transform" should "calculate the chink size" in {
        LubyTransform.calculateChunkCount(10, 10) should equal (1)
        LubyTransform.calculateChunkCount(10, 1) should equal (10)
        LubyTransform.calculateChunkCount(113, 5) should equal (23)
	}

    it should "XOR two byte arrays into a block" in {
        val xor = LubyTransform.xor("abcd", "efgh")

        xor(0) ^ 'a' should equal ('e')
        xor(1) ^ 'b' should equal ('f')
        xor(2) ^ 'c' should equal ('g')
        xor(3) ^ 'd' should equal ('h')

        xor(0) ^ 'e' should equal ('a')
        xor(1) ^ 'f' should equal ('b')
        xor(2) ^ 'g' should equal ('c')
        xor(3) ^ 'h' should equal ('d')
	}

	it should "XOR two disparately sized byte arrays into a block" in {
        val one = LubyTransform.xor("abcd", "efg")

        one.length should equal (4)

        one(0) ^ 'a' should equal ('e')
        one(1) ^ 'b' should equal ('f')
        one(2) ^ 'c' should equal ('g')
        one(3) should equal ('d')

        one(0) ^ 'e' should equal ('a')
        one(1) ^ 'f' should equal ('b')
        one(2) ^ 'g' should equal ('c')

        val two = LubyTransform.xor("abc", "efgh")

        two.length should equal (4)

        two(0) ^ 'a' should equal ('e')
        two(1) ^ 'b' should equal ('f')
        two(2) ^ 'c' should equal ('g')
        two(3) should equal ('h')

        two(0) ^ 'e' should equal ('a')
        two(1) ^ 'f' should equal ('b')
        two(2) ^ 'g' should equal ('c')
	}

    it should "XOR an arbitrary number of byte arrays into a block" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijkl"

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, three)).get

        xor.length should equal (4)

        xor(0) ^ 'a' ^ 'e' should equal ('i')
        xor(1) ^ 'b' ^ 'f' should equal ('j')
        xor(2) ^ 'c' ^ 'g' should equal ('k')
        xor(3) ^ 'd' ^ 'h' should equal ('l')

        xor(0) ^ 'a' ^ 'i' should equal ('e')
        xor(1) ^ 'b' ^ 'j' should equal ('f')
        xor(2) ^ 'c' ^ 'k' should equal ('g')
        xor(3) ^ 'd' ^ 'l' should equal ('h')
    }

    it should "XOR an arbitrary number of disparately sized byte arrays into a block" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijk"

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, three)).get

        xor.length should equal (4)

        xor(0) ^ 'a' ^ 'e' should equal ('i')
        xor(1) ^ 'b' ^ 'f' should equal ('j')
        xor(2) ^ 'c' ^ 'g' should equal ('k')
        xor(3) ^ 'd' should equal ('h')

        xor(0) ^ 'a' ^ 'i' should equal ('e')
        xor(1) ^ 'b' ^ 'j' should equal ('f')
        xor(2) ^ 'c' ^ 'k' should equal ('g')
        xor(3) ^ 'h' should equal ('d')
    }

    it should "XOR a particular set of chunks" in {
        val one = "rnmen"
        val two = "there"
        val combined = "\034\032\101\027\017"
        combined.length should equal (5)

        val result = "nt, t"

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, combined)).get

        xor.length should equal (5)

        xor(0) should equal ('n')
        xor(1) should equal ('t')
        xor(2) should equal (',')
        xor(3) should equal (' ')
        xor(4) should equal ('t')
    }

    it should "decode a chunk from a prepared block" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijk"

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, three)).get
        val block = PreparedBlock(Set(0, 1, 2), LubyTransform.combine(Seq(one, two, three)).get)
        val available = Set(1, 2)
        val buffer = IndexedSeq[Array[Byte]]("    ", two, three)

        new String(buffer(0)) should be ("    ")

        val decoded = LubyTransform.decodeChunk(buffer, block, available)

        decoded.get should be (0)

        new String(buffer(0)) should be (one)
        new String(buffer(1)) should be (two)
        new String(buffer(2)) should be (three)
    }

    it should "not decode a chunk from a prepared block if there are not enough available chunks" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijk"

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, three)).get
        val block = PreparedBlock(Set(0, 1, 2), LubyTransform.combine(Seq(one, two, three)).get)
        val available = Set(1)
        val buffer = IndexedSeq[Array[Byte]]("    ", two, "    ")

        new String(buffer(0)) should be ("    ")

        val decoded = LubyTransform.decodeChunk(buffer, block, available)

        decoded should be (None)

        new String(buffer(0)) should be ("    ")
        new String(buffer(1)) should be (two)
        new String(buffer(2)) should be ("    ")
    }

    it should "consume a stream of byte arrays and create a stream of blocks" in {
        val input: Array[Byte] = "abcdefghijklmnop"

        val fountain = new LubyTransform(wrap(input), 14, 4)
        val output: Stream[Block] = fountain.transform
        
        output should not equal (Empty)

        val first = output.head
        val second = output.tail.head

        first.data.length should equal (4)
        first.data(0) ^ 'e' ^ 'a' ^ 'i' should equal ('m')
        first.data(1) ^ 'f' ^ 'b' ^ 'j' should equal ('n')
        first.data(2) ^ 'g' ^ 'c' ^ 'k' should equal ('o')
        first.data(3) ^ 'h' ^ 'd' ^ 'l' should equal ('p')

        second.data.length should equal (4)
        second.data(0) ^ 'm' ^ 'e' should equal ('i')
        second.data(1) ^ 'n' ^ 'f' should equal ('j')
        second.data(2) ^ 'o' ^ 'g' should equal ('k')
        second.data(3) ^ 'p' ^ 'h' should equal ('l')
    }

    it should "consume a stream of disparately sized byte arrays and create a stream of blocks" in {
        val input: Array[Byte] = "abcdefghijk"

        val fountain = new LubyTransform(wrap(input), 14, 4)
        val output: Stream[Block] = fountain.transform
        
        output should not equal (Empty)

        val first = output.head
        val second = output.tail.head
        val third = output.tail.tail.head

        first.data.length should equal (4)
        first.data(0) ^ 'a' should equal ('e')
        first.data(1) ^ 'b' should equal ('f')
        first.data(2) ^ 'c' should equal ('g')
        first.data(3) ^ 'd' should equal ('h')

        second.data.length should equal (4)
        second.data(0) should equal ('a')
        second.data(1) should equal ('b')
        second.data(2) should equal ('c')
        second.data(3) should equal ('d')

        third.data.length should equal (4)
        third.data(0) ^ 'i' ^ 'a' should equal ('e')
        third.data(1) ^ 'j' ^ 'b' should equal ('f')
        third.data(2) ^ 'k' ^ 'c' should equal ('g')
        third.data(3) ^ 'd' should equal ('h')
    }

    it should "generate a decodable chunk stream from a file" in {
        val source = new File(getClass.getClassLoader.getResource("test.txt").toURI)
        source.length should be (113)

        val destination = File.createTempFile("luby-", ".txt")
        
        import LubyTransform.file2buffer
        val transformer = new LubyTransform(source, 15, 5)
        val stream = transformer.transform
        LubyTransform.read(stream, destination)
        
        val result = Source.fromFile(destination).mkString
        result should be ("When the people fear their government, there is tyranny; when the government fears the people, there is liberty.\n")
    }
}
