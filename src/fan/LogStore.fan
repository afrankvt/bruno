//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   16 Jun 2017  Andy Frank  Creation
//

using concurrent

**
** LogStore is an implementation of Store using an append-only log-structure.
**
internal class LogStore : Store
{

//////////////////////////////////////////////////////////////////////////
// Construction
//////////////////////////////////////////////////////////////////////////

  ** Construct a new LogStore inside the given directory.
  new make(File dir)
  {
    this.dir = dir
    this.mergeLock = dir + `merge.lock`

    // sanity check to clear locks
    if (mergeLock.exists) mergeLock.delete
  }

//////////////////////////////////////////////////////////////////////////
// Store
//////////////////////////////////////////////////////////////////////////

  ** Open store.
  override This open()
  {
    t1 := Duration.now
    if (seg != null) throw IOErr("Store already open")

    // read in existing segments
    last := -1
    segs := listSegments
    segs.each |f|
    {
      last = last.max(f.ext.toInt)
      LogSegment.read(f, bmap)
    }

    // assign Bucket._store
    bmap.vals.each |b| { b._store = this }

    // active segments are always even (merges are odd)
    ext := last + 1
    if (ext.isOdd) ext++

    // create new active segment
    seg = LogSegment(dir + `data.${ext}`)
    seg.open

    t2 := Duration.now
    log.info("opened ($numBuckets buckets, $numRecs recs, ${(t2-t1).toMillis}ms)")
    return this
  }

  ** Close store.
  override This close()
  {
    // TODO: what if merge in progress?

    seg?.close
    bmap.clear
    log.info("closed")
    return this
  }

  ** Return current buckets for this store.
  override Str[] buckets() { bmap.keys }

  ** Return given bucket.
  override Bucket? bucket(Str name, Bool create := false)
  {
    b := bmap[name]
    if (b == null && create) bmap[name] = b = Bucket(name) { it._store=this }
    return b
  }

  ** Callback from `Bucket` to persist a rec change to the given bucket.
  override Void write(Str op, Str bucket, Rec rec, Obj? meta)
  {
    try
    {
      switch (op)
      {
        case "add":    seg.append(bucket, rec).sync
        case "update": seg.append(bucket, rec).sync  // TODO: only changes?
        case "remove": seg.append(bucket, rec, true).sync
        default: throw ArgErr("Unsupported op '$op'")
      }
    }
    // catch (SegmentMaxSizeErr err)
    // {
    //   // write exceeded max log file size; close the active
    //   // segment, open a new active segment, and retry write
    //   closeAndOpenNewSegment
    //   write(op, bucket, rec, meta)
    // }
    catch (Err err)
    {
      // always wrap in IOErr
      throw IOErr("Write failed", err)
    }
  }

  ** Spawn background actor in given 'pool' to compact record data.
  override Future compact(ActorPool pool)
  {
    // return with 'cancel' if a merge already in progress
    if (mergeLock.exists) return Future { it.cancel }

    // return with 'ok' if not enough segments to merge;
    // require at least two inactive segments in order to merge
    segs := listSegments
    if (segs.size < 3) return Future { it.complete(null) }

    // TODO FIXIT: if (cur-list == 1) already merged; return 'ok'

    // create lockfile; gets removed in LogMerge
    mergeLock.create

    // find list of inactive segments
    cur := seg.index
    ext := cur - 1
    inactive := segs.findAll |f| { f.ext.toInt < cur }

    // spawn merge in background actor
    lock := this.mergeLock
    log  := this.log
    mactor = Actor(pool) |m| { merge(inactive, ext, lock, log); return null }
    return mactor.send("go")
  }

//////////////////////////////////////////////////////////////////////////
// Merge
//////////////////////////////////////////////////////////////////////////

  ** Merge inactive segments.
  private static Void merge(File[] inactive, Int ext, File lock, Log log)
  {
    LogSegment? mseg
    try
    {
      t1 := Duration.now
      log.info("compact started")

      // read all records from candidate segments
      osize := 0
      bmap  := Str:Bucket[:]
      inactive.each |f|
      {
        osize += f.size
        LogSegment.read(f, bmap)
      }

      // prune empty buckets since these will not get picked up
      // during the verify vmap phase and throw off size checks
      bkeys := bmap.keys.dup
      bkeys.each |k| { if (bmap[k].recs.size == 0) bmap.remove(k) }

      // delete incomplete merge if found
      mfile := inactive.first.parent + `data.merge`
      if (mfile.exists) mfile.delete

      // TODO: check if out of disk space?

      // write out merged segment
      mseg = LogSegment(mfile)
      mseg.open
      bmap.each |bucket, bname|
      {
        bucket.recs.each |rec|
        {
          mseg.append(bname, rec)
        }
      }
      mseg.sync.close

      // TODO: LogSegment.readEach method that can process a record at
      // a time to avoid having another copy in memory at the same time?

      // read merge back in to verify
      vmap := Str:Bucket[:]
      LogSegment.read(mfile, vmap)

      // verify
      if (bmap.size != vmap.size) throw Err("$bmap.size != $vmap.size")
      bmap.each |mbucket, name|
      {
        vbucket := vmap[name]
        if (!mbucket.verifyEq(vbucket)) throw Err("bucket verify failed for '$name'")
      }

      // verification passed, commit merge, and remove old segments
      mfile = mfile.rename("data.${ext}")
      inactive.each |f| { f.delete }

      t2 := Duration.now
      msize := mfile.size
      dsize := (osize - msize).toLocale("B")

      log.info("compact complete ($inactive.size files, $dsize freed, ${(t2-t1).toMillis}ms)")
    }
    catch (Err err)
    {
      throw IOErr("Compact failed", err)
    }
    finally
    {
      lock.delete
      mseg?.close
    }
  }

//////////////////////////////////////////////////////////////////////////
// Support
//////////////////////////////////////////////////////////////////////////

  ** List current log segments ordered by index.
  private File[] listSegments()
  {
    segs := dir.listFiles.findAll |f|
    {
      f.basename == "data" && f.ext.toInt(10, false) != null
    }
    segs.sort |a,b| { a.ext.toInt <=> b.ext.toInt }
    return segs
  }

  private Int numBuckets() { bmap.size }
  private Int numRecs() { bmap.vals.reduce(0) |Int v,b| { v + b.recs.size } }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private const File dir
  private const File mergeLock

  private LogSegment? seg
  private Str:Bucket bmap := [:]
  private Actor? mactor
}
