package algorithms

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class CombineTest extends FlatSpec with ShouldMatchers {

    implicit def string2bytes (v: String) = v.getBytes("ASCII")

    "The combine function" should "be commutative" in {
        def hex (bytes: Seq[Byte]) = bytes.map{ b => String.format("%02X", java.lang.Byte.valueOf(b)) }.mkString(" ")

        val one: Array[Byte] = "rnmen"
        val two: Array[Byte] = "there"
        val three: Array[Byte] = "nt, t"
        val combined = Array[Byte](0x68, 0x72, 0x24, 0x37, 0x7F)

        val xor = LubyTransform.combine(List[Array[Byte]](one, two, three)).get
        //println("one,two,three: " + hex(xor))
        xor should equal (combined)
        one should equal (string2bytes("rnmen"))
        two should equal (string2bytes("there"))
        three should equal (string2bytes("nt, t"))

        val extract = LubyTransform.combine(List[Array[Byte]](one, two, xor)).get
        //println("one,two,xor: " + new String(extract))
        extract should equal (three)
    }
}
