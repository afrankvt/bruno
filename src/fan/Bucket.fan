//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   20 Jun 2017  Andy Frank  Creation
//

**
** Bucket manages a list of Recs where ids are unique within the buckets
**
internal class Bucket
{
  ** Constructor.
  new make(Str name)
  {
    this.name = name
  }

  ** Name of bucket.
  const Str name

  ** Backing Store for bucket.
  Store store() { _store ?: throw IOErr("Store not configured") }
  internal Store? _store := null

  ** Get an immutable map of all recs by rec id.
  RecMap recs() { map.toImmutable }

  ** Get a rec by id, or null if not found.
  Rec? get(Int id) { map[id] }

  ** Add a new rec to bucket, and return new rec instance. If 'id'
  ** is 'null' then auto-assign a unique id.  Throws 'Err' if 'id'
  ** alread exists in this bucket.
  Rec add(Int? id, Str:Obj? tags)
  {
    // filter out null values
    safe := Str:Obj[:]
    safe.addAll(tags.exclude |v| { v == null })
    rec := Rec(id ?: ++lastId, 0, safe)

    // verify id does not exist
    if (map.get(rec.id) != null)
      throw Err("$name: add id already exist { id=$rec.id }")

    // commit to store first in case it fails
    store.write("add", name, rec, null)
    map = map.set(rec)

    if (isDebug) store.log.debug("[$name:$rec.id] add {" + rec.tags.toStr[1..-2] + "}")
    return rec
  }

  ** Update an existing rec in this bucket, and return updated rec.
  Rec update(Rec rec, Str:Obj? tags)
  {
    // verify rec exists
    old := map.get(rec.id)
    if (old == null) throw ArgErr("$name: Rec not found { id=$rec.id }")

    // merge tags
    copy := rec.tags.rw
    tags.each |v,k|
    {
      if (v == null) copy.remove(k)
      else copy[k] = v
    }

    // TODO: do we concurrent force ver checks here?
    // use old.ver so ver never goes backwards
    mod := Rec(rec.id, old.ver+1, copy)

    // commit to store first in case it fails
    store.write("update", name, mod, null) // tags) TODO
    map = map.set(mod)

    if (isDebug) store.log.debug("[$name:$rec.id] update {" + tags.toStr[1..-2] + "}")
    return mod
  }

  ** Remove an existing rec from this bucket.
  Void remove(Rec rec)
  {
    // verify rec exists
    if (map.get(rec.id) == null)
      throw ArgErr("$name: Rec not found { id=$rec.id }")

    // commit to store first in case it fails
    store.write("remove", name, rec, null)
    map = map.remove(rec.id)

    if (isDebug) store.log.debug("[$name:$rec.id] remove")
  }

  ** Find all recs where 'tag' matches 'val'.
  Rec? find(Str tag, Obj val)
  {
    // TODO: cache/index to avoid linear scans
    // TODO: add eachWhile; or find directly on RecMap
    Rec? found
    map.each |r| { if (r[tag] == val) found = r }
    return found
  }

  // ** Find all recs where 'tag' matches 'val'.
  // RecMap findAll(Str tag, Obj val)
  // {
  //   acc := RecMap
  //   map.each |r| { if (r[tag] == val) acc.add(r) }
  //   return acc
  // }

  ** Verify if two buckets are exactly equal.
  internal Bool verifyEq(Bucket that)
  {
    if (this.recs.size != that.recs.size) return false

    // TODO: need 'all' or 'eachWhile' etc; so we avoid unecessary full scans
    //return this.recs.all |r| { r == that.get(r.id) }

    eq := true
    this.recs.each |r|
    {
      tr := that.get(r.id)
      if (r != tr) eq = false
    }
    return eq
  }

  // Internal hooks to load from Store
  internal Void load(Rec rec)  { map = map.set(rec); lastId = lastId.max(rec.id) }
  internal Void unload(Int id) { map = map.remove(id) }

  private Bool isDebug() { store.log.level == LogLevel.debug }

  private Int lastId := 0
  private RecMap map := RecMap()
}
