package algorithms

import com.github.tototoshi.base64.Base64.encode
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.annotation.tailrec
import scala.collection.immutable.Stream._

class LubyTransformSpec extends FlatSpec with ShouldMatchers {

    implicit def string2bytes (v: String) = v.getBytes("ASCII")

    "A distribution" should "return pseudo-random integers between 1 and bound inclusively" in {
        val stream = LubyTransform.distribution(1, 5)
        stream.head should equal (3)
        stream.tail.head should equal (4)
        stream.tail.tail.head should equal (1)

        @tailrec
        def testBounds (stream: Stream[Int], remaining: Int = 1000): Unit = {
            stream.head should be >= (0)
            stream.head should be < (5)
            testBounds(stream.tail, remaining - 1)
        }
    }

	"A Luby Transform" should "XOR two byte arrays into a block" in {
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

    it should "consume a stream of byte arrays and create a stream of blocks" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijkl"
        val four = "mnop"

        val input = List[Array[Byte]](one, two, three, four)

        val fountain = new LubyTransform(2)
        val output: Stream[Block] = fountain.transform(input)
        
        output should not equal (Empty)

        val first = output.head
        val second = output.tail.head

        first.data.length should equal (4)
        first.data(0) ^ 'i' ^ 'a' should equal ('m')
        first.data(1) ^ 'j' ^ 'b' should equal ('n')
        first.data(2) ^ 'k' ^ 'c' should equal ('o')
        first.data(3) ^ 'l' ^ 'd' should equal ('p')

        second.data.length should equal (4)
        second.data(0) ^ 'i' should equal ('e')
        second.data(1) ^ 'j' should equal ('f')
        second.data(2) ^ 'k' should equal ('g')
        second.data(3) ^ 'l' should equal ('h')
    }

    it should "consume a stream of disparately sized byte arrays and create a stream of blocks" in {
        val one = "abcd"
        val two = "efgh"
        val three = "ijk"

        val input = List[Array[Byte]](one, two, three)

        val fountain = new LubyTransform(2)
        val output: Stream[Block] = fountain.transform(input)
        
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
}
