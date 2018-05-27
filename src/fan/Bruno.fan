//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   16 Jun 2017  Andy Frank  Creation
//

using concurrent
using util

**
** Bruno database interface.
**
const class Bruno
{

//////////////////////////////////////////////////////////////////////////
// Construction
//////////////////////////////////////////////////////////////////////////

  ** Construct a new Bruno instance for the given storage directory.
  new make(File dir)
  {
    this.dir   = dir
    this.pool  = ActorPool { it.name="Bruno" }
    this.actor = Actor(pool) |m| { _receive(m) }
  }

  ** Database dir.
  const File dir

  ** Log file.
  const Log log := Log.get("bruno")

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  ** Open database instance.
  This open()
  {
    actor.send(DbMsg("open", "")).get
    return this
  }

  ** Close database if open, or do nothing if already closed.
  This close()
  {
    actor.send(DbMsg("close", "")).get
    return this
  }

  **
  ** Spawn a background actor to compact record data to reclaim disk space.
  **
  ** Returns [Future]`concurrent::Future` for new actor. 'Future.state' will
  ** be set to 'ok' if compaction completes successfully, or 'err' if fails.
  **
  ** If a compaction is already in process, this method returns immediately
  ** with 'Future.state' set to 'cancelled'.
  **
  Future compact()
  {
    actor.send(DbMsg("compact", "")).get
  }

//////////////////////////////////////////////////////////////////////////
// Recs
//////////////////////////////////////////////////////////////////////////

  ** Get list of buckets in database.
  Str[] buckets()
  {
    m := DbMsg("buckets", "")
    return actor.send(m).get(10sec)
  }

  ** Get a map of all recs in the given bucket, or return an
  ** empty list if the given bucket does not exist.
  RecMap recs(Str bucket)
  {
    m := DbMsg("recs", bucket)
    return actor.send(m).get(10sec)
  }

  ** Get a rec by id from given bucket, or null if rec not found.
  Rec? get(Str bucket, Int id)
  {
    m := DbMsg("get", bucket) { it.id=id }
    return actor.send(m).get(10sec)
  }

  ** Add a new rec to given bucket, and return new rec instance.
  ** Any 'null' values in 'tags' will be skipped.  If 'bucket'
  ** does not exist it will be created.
  Rec add(Str bucket, Str:Obj? tags)
  {
// TODO
//    verifyTypes(tags)
    m := DbMsg("add", bucket) { it.tags=tags.toImmutable }
    return actor.send(m).get(10sec)
  }

  ** Add a new rec to given bucket, but also assign the given id,
  ** and return the new rec instance. Throws 'Err' if a record
  ** already exists with this id.  Any 'null' values in 'tags' will
  ** be skipped. If `bucket` does not exist it will be created.
  @NoDoc Rec addWithId(Str bucket, Int id, Str:Obj? tags)
  {
// TODO
//    verifyTypes(tags)
    m := DbMsg("add", bucket) { it.id=id; it.tags=tags.toImmutable }
    return actor.send(m).get(10sec)
  }

  ** Update an existing rec in the given bucket, and return the
  ** updated instance.  Use 'null' to remove tags from rec.
  Rec update(Str bucket, Rec rec, Str:Obj? tags)
  {
// TODO
//    verifyTypes(tags)
    m := DbMsg("update", bucket) { it.rec=rec; it.tags=tags.toImmutable }
    return actor.send(m).get(10sec)
  }

  ** Remove an existing rec from given bucket.
  Void remove(Str bucket, Rec rec)
  {
    m := DbMsg("remove", bucket) { it.rec=rec }
    actor.send(m).get(10sec)
  }

  **
  ** Find the first record in given bucket where 'tag' equals 'val',
  ** or 'null' for not matches. Supported options:
  **
  **   - '"nocase":true' - check if strings are equal ignoring case
  **
  Rec? find(Str bucket, Str tag, Obj val, [Str:Obj]? opts := null)
  {
    m := DbMsg("find", bucket) { it.tag=tag; it.val=val; it.opts=opts?.toImmutable }
    return actor.send(m).get(10sec)
  }

//////////////////////////////////////////////////////////////////////////
// Actor
//////////////////////////////////////////////////////////////////////////

  private Obj? _receive(DbMsg m)
  {
    Store? store := Actor.locals["store"]

    // check for open
    if (m.op == "open")
    {
      if (store != null) throw IOErr("Database already open")
      Actor.locals["store"] = store = LogStore(dir)
      store.log = log
      store.open
      return null
    }

    // check for close
    if (m.op == "close")
    {
      store?.close
      Actor.locals.remove("store")
      return null
    }

    // remaining ops require open database
    if (store == null) throw IOErr("Database not open")

    switch (m.op)
    {
      case "buckets": return store.buckets.toImmutable
      case "recs":    return store.bucket(m.name)?.recs ?: RecMap.empty
      case "get":     return store.bucket(m.name)?.get(m.id)
      case "add":     return store.bucket(m.name, true).add(m.id, m.tags)
      case "update":  return store.bucket(m.name).update(m.rec, m.tags)
      case "remove":  store.bucket(m.name).remove(m.rec)
      case "find":    return store.bucket(m.name).find(m.tag, m.val, m.opts)
      // case "findAll": return _list(m.name).findAll(m.tag, m.val)
      case "compact": return store.compact(pool)
    }

    return null
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private const ActorPool pool
  private const Actor actor
}

**************************************************************************
** DbMsg
**************************************************************************

internal const class DbMsg
{
  new make(Str op, Str name, |This|? f := null)
  {
    this.op = op
    this.name = name
    if (f != null) f(this)
  }
  const Str op
  const Str name
  const Int? id
  const Rec? rec
  const [Str:Obj?]? tags
  const Str? tag
  const Obj? val
  const [Str:Obj]? opts
}
