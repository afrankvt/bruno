//
// Copyright (c) 2013, Andy Frank
// Licensed under the MIT License
//
// History:
//   15 Mar 2013  Andy Frank  Creation
//

**
** Rec models a set of name/value tags.
**
@Js const class Rec
{
  ** Internal constructor.
  @NoDoc new make(Int id, Int ver, Str:Obj tags)
  {
    // sanity checks
    if (id  < 1 || id  > 0xffff_ffff) throw ArgErr("Rec.id out of bounds: $id")
    if (ver < 0 || ver > 0xffff_ffff) throw ArgErr("Rec.ver out of bounds: $ver")

    this.id = id
    this.ver = ver
    this.tags = tags
  }

  ** Unique id for this rec within a `Bucket`.
  const Int id

  ** Version of this rec.  Each modification to this rec
  ** inside a `Bucket` will increment this value.
  const Int ver

  ** Tag values for this rec.
  const Str:Obj tags

  ** Return a new Map instance which combines `id` and `ver` with `tags`.
  ** The 'id' and 'ver' values get keyed as "__id" and "__ver" to avoid
  ** naming collisions.
  @NoDoc Str:Obj _toMap()
  {
// TODO: this is maybe a bit confusing with ^^^ tags; sorta thinking a better
// design is to standardize on __x as "transient" tags which are never persisted
// on disk; and them just always include __id, __ver?  Might make stuff like
// tags.each { ... } more awkward; not sure...
    Str:Obj[:]
      .add("__id",  id)
      .add("__ver", ver)
      .addAll(tags)
  }

  ** Convenience for `tags.containsKey`.
  Bool has(Str name) { tags.containsKey(name) }

  ** Convenience for `tags.get`
  @Operator Obj? get(Str name) { tags[name] }

  ** Trap operartor is overriden to work as convience for `get`:
  **   rec->foo === rec["foo"]
  override Obj? trap(Str name, Obj?[]? val := null) { tags[name] }

  ** Hash is 'id << 32 | ver'.
  override Int hash() { id.shiftl(32).or(ver) }

  ** Recs are equal if `id`, `ver`, and `tags` are equal.
  override Bool equals(Obj? that)
  {
    x := that as Rec
    if (x == null) return false

    if (id  != x.id) return false
    if (ver != x.ver) return false

    // we need to manually check maps since ConstBuf.equals
    // is implemented with refrence equality, which will
    // most always fail when using Map.equals

    keys  := tags.keys.sort
    xkeys := x.tags.keys.sort
    if (keys != xkeys) return false
    return keys.all |k|
    {
      tv := tags[k]
      xv := x.tags[k]
      if (tv is Buf && xv is Buf)
      {
        // TODO: we can't use pos to check each byte; is checksum good enough? SHA-1??
        tb := (Buf)tv
        xb := (Buf)xv
        return tb.size == xb.size && tb.crc("CRC-32") == xb.crc("CRC-32")
      }
      return tv == xv
    }
  }

  ** toStr returns "$id/$ver { $tags }".
  override Str toStr()
  {
    "$id/$ver { " + tags.toStr[1..-2] + " }"
  }
}