//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   16 Jun 2017  Andy Frank  Creation
//

using concurrent

**
** PerfTest
**
class PerfTest
{
  Void main()
  {
    // ROUGH GOALS: 10_000  adds/s    ~= 0.1ms/op
    //              10_000+ updates/s ~= 0.1ms/op

    rec := [
      "int":     3,
      "str":     "All work and no play makes Jack a dull boy. :)",
      "street":  "1500 W Main St",
      "city":    "Riverdale",
      "state":   "MI",
      "zip":     "90210",
      "created": Date.today.toStr,  // TODO
    ]

    // run("adds", 100)      |db| { doAdds(db, 100,       rec) }
    // run("adds", 1000)     |db| { doAdds(db, 1000,      rec) }
    // run("adds", 10_000)   |db| { doAdds(db, 10_000,    rec) }
    // run("adds", 100_000)  |db| { doAdds(db, 100_000,   rec) }
    // run("adds", 250_000)  |db| { doAdds(db, 250_000,   rec) }
    // run("adds", 1_00_000) |db| { doAdds(db, 1_000_000, rec) }  <-- 10x slowdown? GC?

    run("add_list", 10_000)   |db| { doAddList(db, 10_000,    rec) }
    run("add_list", 100_000)  |db| { doAddList(db, 100_000,   rec) }
    run("add_list", 250_000)  |db| { doAddList(db, 250_000,   rec) }
  }

  Void run(Str name, Int runs, |Bruno| f)
  {
    tempDir.delete
    Actor.sleep(500ms)
    db := Bruno(tempDir)
    db.open

    echo("-- Running ${name} @ ${runs}...")
    start := Duration.now
    f(db)
    end := Duration.now

    db.close
    dur := (end-start).toMillis
    op  := dur.toFloat / runs.toFloat
    num := runs.toFloat / (dur.toFloat / 1000f)

    data := 0
    tempDir.listFiles.each |s| { data += s.size }

    echo("    time:  ${dur}ms  " + op.toLocale("0.000") + "ms/op  ${num.toInt} ops/s")
    echo("    data:  " + data.toLocale("B") + "  ($data.toLocale bytes)")

    // measure reload time
    start = Duration.now
    db.open
    end = Duration.now
    dur = (end-start).toMillis
    echo("    load:  ${dur}ms")
    echo("")
  }

  Void doAdds(Bruno db, Int runs, Str:Obj tags)
  {
    runs.times { db.add("test_bucket", tags) }
  }

  Void doAddList(Bruno db, Int runs, Str:Obj tags)
  {
    runs.times { db.add("test_bucket", tags) }
    runs.times { x := db.recs("test_bucket") }
  }

  File tempDir := Env.cur.workDir + `temp/perf/`
}