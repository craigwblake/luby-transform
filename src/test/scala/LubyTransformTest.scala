package algorithms

import com.github.tototoshi.base64.Base64.encode
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection.immutable.Stream._

class LubyTransformSpec extends FlatSpec with ShouldMatchers {

	"A Luby transform" should "XOR two byte arrays into a block" in {
        val one = "abcd".getBytes("ASCII")
        val two = "efgh".getBytes("ASCII")

        val xor = LubyTransform.xor(one, two)

        xor(0) ^ 'a' should equal ('e')
        xor(1) ^ 'b' should equal ('f')
        xor(2) ^ 'c' should equal ('g')
        xor(3) ^ 'd' should equal ('h')

        xor(0) ^ 'e' should equal ('a')
        xor(1) ^ 'f' should equal ('b')
        xor(2) ^ 'g' should equal ('c')
        xor(3) ^ 'h' should equal ('d')
	}

    it should "XOR an arbitrary number of byte arrays into a block" in {
        val one = "abcd".getBytes("ASCII")
        val two = "efgh".getBytes("ASCII")
        val three = "ijkl".getBytes("ASCII")

        val xor = LubyTransform.combine(one, Stream(two, three), 2)

        xor._2 should equal (Empty)

        xor._1(0) ^ 'a' ^ 'e' should equal ('i')
        xor._1(1) ^ 'b' ^ 'f' should equal ('j')
        xor._1(2) ^ 'c' ^ 'g' should equal ('k')
        xor._1(3) ^ 'd' ^ 'h' should equal ('l')

        xor._1(0) ^ 'a' ^ 'i' should equal ('e')
        xor._1(1) ^ 'b' ^ 'j' should equal ('f')
        xor._1(2) ^ 'c' ^ 'k' should equal ('g')
        xor._1(3) ^ 'd' ^ 'l' should equal ('h')
    }

    it should "consume a stream of byte arrays and create a stream of blocks" in {
        val one = "abcd".getBytes("ASCII")
        val two = "efgh".getBytes("ASCII")
        val three = "ijkl".getBytes("ASCII")
        val four = "mnop".getBytes("ASCII")

        val input = cons(one, cons(two, cons(three, Stream(four))))

        // Magic number which seeds the test to encode two blocks
        val fountain = new LubyTransform(1)
        val output: Stream[Block] = fountain.transform(input)
        
        output should not equal (Empty)
        output.tail.tail should equal (Empty)

        val first = output.head
        val second = output.tail.head

        first.data.length should equal (4)
        first.data(0) ^ 'a' should equal ('e')
        first.data(1) ^ 'b' should equal ('f')
        first.data(2) ^ 'c' should equal ('g')
        first.data(3) ^ 'd' should equal ('h')

        second.data.length should equal (4)
        second.data(0) ^ 'i' should equal ('m')
        second.data(1) ^ 'j' should equal ('n')
        second.data(2) ^ 'k' should equal ('o')
        second.data(3) ^ 'l' should equal ('p')
    }
}
