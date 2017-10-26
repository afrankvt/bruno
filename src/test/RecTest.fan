//
// Copyright (c) 2013, Andy Frank
// Licensed under the MIT License
//
// History:
//   15 Mar 2013  Andy Frank  Creation
//

**
** RecTest
**
class RecTest : Test
{
  Void testIdentity()
  {
    a := Rec(1, 0, [:])
    b := Rec(1, 0, ["foo":5])
    c := Rec(2, 0, [:])
    d := Rec(3, 0, ["foo":10, "bar":3, "zoo":false])

    verifyEq(a, Rec(1, 0, a.tags))
    verifyEq(b, Rec(1, 0, b.tags))
    verifyEq(c, Rec(2, 0, c.tags))
    verifyEq(d, Rec(3, 0, ["bar":3, "zoo":false, "foo":10]))

    verifyEq(a, a)
    verifyNotEq(a, b)
    verifyNotEq(a, c)

    verifyNotEq(a, Rec(2, 0, [:]))
    verifyNotEq(a, Rec(1, 0, ["x":10]))
    verifyNotEq(d, Rec(3, 0, ["bar":4, "zoo":true, "foo":11]))

    verifyEq(b.has("foo"), true)
    verifyEq(b.get("foo"), 5)
    verifyEq(b->foo,       5)

    verifyEq(c.has("foo"), false)
    verifyEq(c.get("foo"), null)
    verifyEq(c->foo,       null)

    am := Rec(1, 1, [:])
    bm := Rec(1, 6, ["foo":5])
    cm := Rec(2, 3, [:])

    verifyNotEq(a, am)
    verifyNotEq(b, bm)
    verifyNotEq(c, cm)

    big1 := Rec(0xffff_fffe, 0xffff_fffe, ["foo":12])
    big2 := Rec(0xffff_ffff, 0xffff_ffff, ["foo":12])
    verifyEq(big1.id,  0xffff_fffe)
    verifyEq(big1.ver, 0xffff_fffe)
    verifyEq(big2.id,  0xffff_ffff)
    verifyEq(big2.ver, 0xffff_ffff)

    verifyErr(ArgErr#) { x := Rec(0, 0, [:]) }               // id out of bounds
    verifyErr(ArgErr#) { x := Rec(-10, 0, [:]) }             // id out of bounds/ no id
    verifyErr(ArgErr#) { x := Rec(0xffff_ffff+1, 0, [:]) }   // id out of bounds

    verifyErr(ArgErr#) { x := Rec(10, -1,   [:]) }           // ver out of bounds
    verifyErr(ArgErr#) { x := Rec(10, -100, [:]) }           // ver out of bounds
    verifyErr(ArgErr#) { x := Rec(10, 0xffff_ffff+1, [:]) }  // ver out of bounds
  }
}
