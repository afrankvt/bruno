//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   16 Jun 2017  Andy Frank  Creation
//

using concurrent

**
** Store handles persistance for a Bruno database.
**
internal abstract class Store
{
  ** Open store.
  abstract This open()

  ** Close store.
  abstract This close()

  ** Return current buckets for this store.
  abstract Str[] buckets()

  ** Return the given Bucket. If bucket does not exist and `create` is
  ** false, return null, otherwise create a new Bucket with `name`.
  abstract Bucket? bucket(Str name, Bool create := false)

  ** Callback from `Bucket` to persist a rec change to the given bucket.
  ** Throw 'IOErr' if write could not be completed.
  abstract Void write(Str op, Str bucket, Rec rec, Obj? meta)

  ** Spawn background actor in given 'pool' to compact record data.
  ** Returns Future from new background actor.
  abstract Future compact(ActorPool pool)

  // set in Bruno._receive:open
  internal Log? log
}
