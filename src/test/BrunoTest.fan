//
// Copyright (c) 2012, Andy Frank
// Licensed under the MIT License
//
// History:
//   8 Aug 2012  Andy Frank  Creation
//

**
** BrunoTest
**
class BrunoTest : Test
{

//////////////////////////////////////////////////////////////////////////
// Empty
//////////////////////////////////////////////////////////////////////////

  Void testEmpty()
  {
    db := Bruno(tempDir)
    db.open

    verifyEq(db.buckets.size, 0)

    verifyEq(db.recs("a").size, 0)
    verifyEq(db.recs("b").size, 0)
    verifyEq(db.recs("c").size, 0)

    verifyEq(db.recs("a")[100], null)
    verifyEq(db.recs("foo")[5], null)
    verifyEq(db.get("a", 100),  null)
    verifyEq(db.get("foo", 5),  null)

    verifyEq(db.buckets.size, 0)

    db.close
    verifyErr(IOErr#) { db.buckets }
    verifyErr(IOErr#) { db.recs("foo") }
  }

//////////////////////////////////////////////////////////////////////////
// Add
//////////////////////////////////////////////////////////////////////////

  Void testAdd()
  {
    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    verifyEq(db.recs("a"), RecMap.empty)
    verifyEq(db.recs("b"), RecMap.empty)
    verifyEq(db.recs("c"), RecMap.empty)

    one := db.add("a", ["int":3, "str":"foo"])
    verifyEq(db.buckets.size, 1)
    verifyEq(db.recs("a").size, 1)
    verifyEq(db.recs("a")[1], one)
    verifyEq(db.get("a", 1),  one)

    two := db.add("a", ["int":5, "str":"bar"])
    verifyEq(db.recs("a").size, 2)
    verifyEq(db.recs("a")[2], two)
    verifyEq(db.get("a", 2),  two)

    // // invalid types
    // verifyErr(ArgErr#) { db.add("a", ["x":StrBuf()]) }
    // verifyErr(ArgErr#) { db.add("a", ["x":`www.google.com`]) }

    // reload database
    db.close
    db.open

    verifyEq(db.buckets.size, 1)
    verifyEq(db.recs("a").size,  2)
    verifyEq(db.get("a", 1), one)
    verifyEq(db.get("a", 2), two)

    // // test nulls
    // nulls := db.add("a", ["int":8, "str":"xx", "skip":null])
    // verifyEq(db.list("a").size, 3)
    // verifyEq(db.list("a")[2], nulls)
    // verifyEq(db.get("a", 3),  nulls)
    // verifyEq(db.get("a", 3).has("skip"), false)

    db.close
  }

  Void testAddWithId()
  {
    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    verifyEq(db.recs("x"), RecMap.empty)

    a := db.add("x", ["int":3, "str":"foo"])
    verifyEq(db.recs("x").size, 1)
    verifyEq(db.get("x", 1), a)

    b := db.add("x", ["int":5, "str":"bar"])
    verifyEq(db.recs("x").size, 2)
    verifyEq(db.get("x", 2), b)

    c := db.addWithId("x", 100, ["int":7, "str":"yaz"])
    verifyEq(db.recs("x").size, 3)
    verifyEq(db.get("x", 100), c)

    d := db.add("x", ["int":9, "str":"zoo"])
    verifyEq(db.recs("x").size, 4)
    verifyEq(db.get("x", 3), d)

    verifyErr(Err#) { db.addWithId("x", 1,   [:]) }  // id exists
    verifyErr(Err#) { db.addWithId("x", 100, [:]) }  // id exists

    // TODO: autoincrement and addWithId checks??
  }

//////////////////////////////////////////////////////////////////////////
// Update
//////////////////////////////////////////////////////////////////////////

  Void testUpdate()
  {
    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    verifyEq(db.recs("a"), RecMap.empty)

    one := db.add("a", ["int":3, "str":"foo"])
    verifyEq(db.recs("a").size,  1)
    verifyEq(db.recs("a")[1],  one)
    verifyEq(db.get("a", 1),  one)
    verifyEq(db.get("a", 1).tags["int"], 3)
    verifyEq(db.get("a", 1).tags["str"], "foo")

    oneUp := db.update("a", one, ["int":7, "str":"bar", "x":false])
    verifyEq(db.recs("a").size,  1)
    verifyEq(db.recs("a")[1], oneUp)
    verifyEq(db.get("a", 1),  oneUp)
    verifyEq(db.get("a", 1).tags["int"], 7)
    verifyEq(db.get("a", 1).tags["str"], "bar")
    verifyEq(db.get("a", 1).tags["x"],   false)

    // rec not found
    verifyErr(ArgErr#) { db.update("a", Rec(8, 0, [:]), ["int":2]) }

    // // invalid types
    // verifyErr(ArgErr#) { db.add("a", ["x":StrBuf()]) }
    // verifyErr(ArgErr#) { db.add("a", ["x":`www.google.com`]) }

    // reload database
    db.close
    db.open

    verifyEq(db.recs("a").size, 1)
    verifyEq(db.recs("a")[1], oneUp)
    verifyEq(db.get("a", 1),  oneUp)
    verifyEq(db.get("a", 1).tags["int"], 7)
    verifyEq(db.get("a", 1).tags["str"], "bar")

    // test using update with out-of-date copy
    oneUp2 := db.update("a", one, ["int":5])
    verifyEq(db.recs("a").size,  1)
    verifyEq(db.recs("a")[1], oneUp2)
    verifyEq(db.get("a", 1),  oneUp2)
    verifyEq(db.get("a", 1).tags["int"], 5)
    verifyEq(db.get("a", 1).tags["str"], "bar")
    verifyEq(db.get("a", 1).tags["x"],   false)

    db.close
  }

//////////////////////////////////////////////////////////////////////////
// Remove
//////////////////////////////////////////////////////////////////////////

  Void testRemove()
  {
    // TODO: need to verify segment.x load order!

    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    verifyEq(db.recs("a"), RecMap.empty)
    verifyEq(db.recs("a").size, 0)

    one   := db.add("a", ["int":11, "str":"alpha"])
    two   := db.add("a", ["int":12, "str":"beta"])
    three := db.add("a", ["int":13, "str":"gamma"])
    four  := db.add("a", ["int":14, "str":"delta"])
    verifyEq(db.recs("a").size, 4)

    db.remove("a", three)
    verifyErr(ArgErr#) { db.remove("a", Rec(10, 0, [:])) }
    verifyEq(db.recs("a").size,  3)

    // reload database
    db.close
    db.open

    verifyEq(db.recs("a").size,  3)

    db.close
  }

//////////////////////////////////////////////////////////////////////////
// Find
//////////////////////////////////////////////////////////////////////////

  Void testFind()
  {
    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    a1 := db.add("a", ["int":11, "str":"x"])
    a2 := db.add("a", ["int":12, "str":"y"])
    a3 := db.add("a", ["int":13, "str":"z"])

    verifyEq(db.find("a", "str",    "x"),   a1)
    verifyEq(db.find("a", "str",    "y"),   a2)
    verifyEq(db.find("a", "str",    "z"),   a3)
    verifyEq(db.find("a", "str", "none"), null)
    verifyEq(db.find("a", "int",     12),   a2)
    verifyEq(db.find("a", "xxx",  false), null)

    db.close
  }

  Void testFindOpts()
  {
    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    a1 := db.add("a", ["int":11, "str":"FooBar"])
    a2 := db.add("a", ["int":12, "str":"Cool #15"])

    verifyEq(db.find("a", "str", "foobar"), null)
    verifyEq(db.find("a", "str", "foobar", ["nocase":true]), a1)

    verifyEq(db.find("a", "str", "cool #15"), null)
    verifyEq(db.find("a", "str", "cool #15", ["nocase":true]), a2)

    db.close
  }

  // Void testFindAll()
  // {
  //   db := Bruno(tempDir) { log.level = LogLevel.debug }
  //   db.open
  //
  //   a1 := db.add("a", ["int":11, "str":"foo"])
  //   a2 := db.add("a", ["int":12, "str":"foo"])
  //   a3 := db.add("a", ["int":13, "str":"foo"])
  //   a4 := db.add("a", ["int":14, "str":"bar"])
  //
  //   verifyEq(db.findAll("a", "str", "foo"),  Rec[a1,a2,a3])
  //   verifyEq(db.findAll("a", "str", "bar"),  Rec[a4])
  //   verifyEq(db.findAll("a", "str", "none"), Rec[,])
  //   verifyEq(db.findAll("a", "str", 55),     Rec[,])
  //   verifyEq(db.findAll("a", "id",  1),      Rec[a1])
  //
  //   // verifyErr(ArgErr#) { db.findAll("b", "id", 5) }
  //
  //   db.close
  // }

//////////////////////////////////////////////////////////////////////////
// Types
//////////////////////////////////////////////////////////////////////////

  Void testTypes()
  {
    // TODO: lots more :)

    // Str/Uri: unicode / equality
    // Buf -- check equality

    // TODO: test multiple Buf vals

    db := Bruno(tempDir) { log.level = LogLevel.debug }
    db.open

    tags := [
      "str":   "foobar",
      "bool":  true,
      "int":   12,
      "float": 3.14f,
      "uri":   `http://fantom.org`,
      "buf1":  Buf().writeI8(0x5678_abcd_1234_ffee),
      "buf2":  Buf().writeI8(0xabcd_ef00_1234_8765),
      "date1": Date(2017, Month.jul, 31),
      "date2": Date(1950, Month.dec,  1),
      "time1": Time(2, 30, 00),
      "time2": Time(23, 59, 59),
      "ts":    DateTime("2017-10-13T13:29:47.643-04:00 New_York"),
    ]

    rec := db.add("test", tags)
    verifyEq(db.recs("test").size, 1)

    db.close
    db.open

    test := db.get("test", rec.id)
    verifyEq(test->str,   "foobar")
    verifyEq(test->bool,  true)
    verifyEq(test->int,   12)
    verifyEq(test->float, 3.14f)
    verifyEq(test->uri,   `http://fantom.org`)
    verifyEq(test->date1, Date(2017, Month.jul, 31))
    verifyEq(test->date2, Date(1950, Month.dec, 1))
    verifyEq(test->time1, Time(2, 30, 00))
    verifyEq(test->time2, Time(23, 59, 59))
    verifyEq(test->ts,    DateTime("2017-10-13T13:29:47.643-04:00 New_York").toUtc)

    verify(test->buf1 is Buf)
    Buf buf1 := test->buf1
    verifyEq(buf1.size, 8)
    verifyEq(buf1.in.readS8, 0x5678_abcd_1234_ffee)

    verify(test->buf2 is Buf)
    Buf buf2 := test->buf2
    verifyEq(buf2.size, 8)
    verifyEq(buf2.in.readS8, 0xabcd_ef00_1234_8765)
  }
}