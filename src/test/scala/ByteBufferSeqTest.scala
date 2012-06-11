package algorithms

import java.nio._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class ByteBufferSeqTest extends FlatSpec with ShouldMatchers {

    implicit def string2bytes (v: String) = v.getBytes("ASCII")

    def hex (bytes: Seq[Byte]) = bytes.map{ b => String.format("%02X", java.lang.Byte.valueOf(b)) }.mkString(" ")

    "The byte buffer seq" should "read from a buffer" in {
        val text = "this is a test"
        val buffer = ByteBuffer.wrap(text)
        val wrapper = ByteBufferSeq(buffer, 4)

        new String(wrapper(0)) should be ("this")
        new String(wrapper(1)) should be (" is ")
        new String(wrapper(2)) should be ("a te")
        new String(wrapper(3)) should be ("st")
        wrapper(3) should be (string2bytes("st"))
    }

    it should "write to a buffer" in {

        val text = "this is a test"
        val replace = "two three four"
        val buffer = ByteBuffer.wrap(text)
        val wrapper = ByteBufferSeq(buffer, 4)

        new String(wrapper(0)) should be ("this")
        new String(wrapper(1)) should be (" is ")
        new String(wrapper(2)) should be ("a te")
        new String(wrapper(3)) should be ("st")

        wrapper(0) = "two "

        new String(wrapper(0)) should be ("two ")
        new String(wrapper(1)) should be (" is ")
        new String(wrapper(2)) should be ("a te")
        new String(wrapper(3)) should be ("st")

        wrapper(1) = "thre"
        wrapper(2) = "e fo"
        wrapper(3) = "ur"

        new String(wrapper(0)) should be ("two ")
        new String(wrapper(1)) should be ("thre")
        new String(wrapper(2)) should be ("e fo")
        new String(wrapper(3)) should be ("ur")
    }
}
