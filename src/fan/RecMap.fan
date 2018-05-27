//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   9 Jul 2017  Andy Frank  Creation
//

**************************************************************************
** RecMap
**************************************************************************

**
** RecMap is an immutable peristent map of ids to `Rec` instances.
** Each mutation operation - `add`, `set`, `remove` - returns a new
** map instance reflecting the modification.  The previous map
** instance remains unmodified.
**
** This class is based on Clojure's PersistentHashMap implemenation:
**
** `https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/PersistentHashMap.java`
**
@Js const class RecMap
{
  ** Empty map ingleton.
  static const RecMap empty := RecMap.privMake(null, 0)

  ** Default constructor is a convenience for `empty`.
  static new make() { empty }

  ** Private ctor.
  private new privMake(RNode? root, Int sz)
  {
    this.root = root
    this.sz   = sz
  }

  ** Number of entries in this map.
  Int size() { sz }

  ** Does this map contain the given id?
  Bool has(Int id)
  {
    root==null ? false : root.get(0, id) != null
  }

  ** Get the `Rec` with given id, or return 'null' if not found.
  @Operator Rec? get(Int id)
  {
    root==null ? null : root.get(0, id)
  }

  ** Return a new map with the given 'rec' added by 'rec.id',
  ** or throw Err if 'id' already exists.
  RecMap add(Rec rec)
  {
    if (has(rec.id)) throw Err("Rec id already exists: $rec.id")
    return set(rec)
  }

  ** Return a new map with the given 'rec' set using 'rec.id' as key.
  RecMap set(Rec val)
  {
    key := val.id
    addedLeaf := Box(null)
    RNode newroot := (root == null ? BitmapIndexedNode.empty : root)
    newroot = newroot.set(0, key, val, addedLeaf)

    if (newroot === root) return this

    return privMake(newroot, addedLeaf.val == null ? sz : sz+1)
  }

  ** Return a new map with record with given 'id' removed. Returns
  ** 'this' if record did not exist.
  RecMap remove(Int id)
  {
    if (root == null) return this

    newroot := root.remove(0, id)
    if (newroot === root) return this

    return privMake(newroot, sz-1)
  }

  // ** TODO
  Void each(|Rec| f)
  {
    if (root == null) return
    root.each(f)
  }

  private const RNode? root
  private const Int sz
}

**************************************************************************
** Box
**************************************************************************

@Js internal class Box
{
  new make(Obj? val) { this.val = val }
  Obj? val
}

**************************************************************************
** RNode
**************************************************************************

@Js internal const mixin RNode
{
  abstract Rec? get(Int shift, Int key)
  abstract RNode set(Int shift, Int key, Rec val, Box addedLeaf)
  abstract RNode? remove(Int shift, Int key)
  abstract Void each(|Rec| f)

  ** Shift and mask the given hash to lower 5-bits.
  protected Int mask(Int hash, Int shift)
  {
    // return (hash >>> shift) & 0x01f
    hash.shiftr(shift).and(0x1f)
  }

  ** Duplicate `list` and set given index to 'a'.
  protected Obj[] dupSet(Obj?[] list, Int i, Obj a)
  {
    dup := list.dup
    dup[i] = a
    return dup
  }

  ** Duplicate `list` and set given indexes to 'a' and 'b'.
  protected Obj[] dupSet2(Obj?[] list, Int i, Obj? a, Int j, Obj b)
  {
    dup := list.dup
    dup[i] = a
    dup[j] = b
    return dup
  }

  ** Remove key/val pair at index 'i'.
  protected Obj[] remPair(Obj[] list, Int i)
  {
    // TODO: this performs 2 arraycopys (removeAt, removeAt)
    // Can we reduce to 1 without using FFI ??
    dup := list.dup
    dup.removeAt(2 * i)
    dup.removeAt(2 * i)
    return dup
  }
}

**************************************************************************
** ListNode
**************************************************************************

@Js internal const class ListNode : RNode
{
  ** ListNode constructor.
  new make(Int sz, RNode[] list)
  {
    this.sz = sz
    this.list = list
  }

  ** Set impl for ListNode
  override Rec? get(Int shift, Int key)
  {
    idx  := mask(key.hash, shift)
    node := list[idx]
    if (node == null) return null
    return node.get(shift+5, key)
  }

  ** Set impl for ListNode
  override RNode set(Int shift, Int key, Rec val, Box addedLeaf)
  {
    idx  := mask(key.hash, shift)
    node := list[idx]

    if (node == null)
      return ListNode(sz+1,
        dupSet(list, idx, BitmapIndexedNode.empty.set(shift+5, key, val, addedLeaf)))

    n := node.set(shift+5, key, val, addedLeaf)
    if (n == node) return this

    return ListNode(sz, dupSet(list, idx, n))
  }

  ** Remove impl for ListNode
  override RNode? remove(Int shift, Int key)
  {
    idx  := mask(key.hash, shift)
    node := list[idx]

    if (node == null) return this

    n := node.remove(shift + 5, key)
    if (n == node) return this

    if (n == null)
    {
      if (sz <= 8) return pack(idx)
      return ListNode(sz-1, dupSet(list, idx, n))
    }
    else
    {
      return ListNode(sz, dupSet(list, idx, n))
    }
  }

  ** Compact to new index node.
  private RNode pack(Int idx)
  {
    newArray := List.make(Obj#, 2 * (sz-1))
    j := 1
    bitmap := 0

    for (i:=0; i<idx; i++)
    {
      if (list[i] != null)
      {
        newArray[j] = list[i]
        bitmap = bitmap.or(1.shiftl(i))
        j += 2
      }
    }

    for (i:=idx+1; i<list.size; i++)
    {
      if (list[i] != null)
      {
        newArray[j] = list[i]
        bitmap = bitmap.or(1.shiftl(i))
        j += 2
      }
    }

    return BitmapIndexedNode(bitmap, newArray)
  }

  ** Each impl for ListNode.s
  override Void each(|Rec| f)
  {
    i := 0
    while (true)
    {
      if (i < list.size)
      {
        RNode? node := list[i++]
        if (node != null) node.each(f)
      }
      else return
    }
  }

  private const Int sz
  private const RNode?[] list
}

**************************************************************************
** BitmapIndexedNode
**************************************************************************

@Js internal const class BitmapIndexedNode : RNode
{
  ** Empty singleton.
  static const BitmapIndexedNode empty := make(0, Obj#.emptyList)

  ** BitmapIndexedNode constructor.
  new make(Int bitmap, Obj[] list)
  {
    this.bitmap = bitmap
    this.list   = list
  }

  ** Get impl for BitmapIndexedNode
  override Rec? get(Int shift, Int key)
  {
    bit := bitpos(key.hash, shift)
    if (bitmap.and(bit) == 0) return null

    idx := index(bit)
    keyOrNull := list[2*idx]
    valOrNode := list[2*idx+1]

    if(keyOrNull == null)
      return ((RNode)valOrNode).get(shift+5, key)

    if (key == keyOrNull) return valOrNode

    return null
  }

  ** Set impl for BitmapIndexedNode
  override RNode set(Int shift, Int key, Rec val, Box addedLeaf)
  {
    bit := bitpos(key.hash, shift)
    idx := index(bit)

    if ((bitmap.and(bit)) != 0)
    {
      keyOrNull := list[2*idx]
      valOrNode := list[2*idx+1]

      if (keyOrNull == null)
      {
        RNode n := ((RNode) valOrNode).set(shift+5, key, val, addedLeaf)
        if (n == valOrNode) return this
        return BitmapIndexedNode(bitmap, dupSet(list, 2*idx+1, n))
      }

      if (key == keyOrNull)
      {
        if (val == valOrNode) return this
        return BitmapIndexedNode(bitmap, dupSet(list, 2*idx+1, val))
      }

      addedLeaf.val = addedLeaf
      return BitmapIndexedNode(bitmap, dupSet2(list,
        2*idx, null,
        2*idx+1, createNode(shift+5, keyOrNull, valOrNode, key, val)
      ))
    }
    else
    {
      n := bitCount(bitmap)
      if (n >= 16)
      {
        nodes := RNode?[,] { it.size=32 }
        jdx   := mask(key.hash, shift)
        nodes[jdx] = empty.set(shift + 5, key, val, addedLeaf)
        j := 0
        for (i:=0; i<32; i++)
        {
          if (bitmap.shiftr(i).and(1) != 0)
          {
            if (list[j] == null)
              nodes[i] = list[j+1]
            else
              nodes[i] = empty.set(shift + 5, list[j], list[j+1], addedLeaf)
            j += 2
          }
        }
        return ListNode(n + 1, nodes)
      }
      else
      {
        // TODO: this performs 3 arraycopys (dup, insert, insert)
        // Can we reduce to 1 without using FFI ??

        newList := list.dup
        newList.capacity = 2 * (n+1)
        newList.insert(2*idx, key)
        newList.insert(2*idx+1, val)
        addedLeaf.val = addedLeaf
        return BitmapIndexedNode(bitmap.or(bit), newList)
      }
    }
  }

  ** Remove impl for BitmapIndexedNode
  override RNode? remove(Int shift, Int key)
  {
    bit := bitpos(key.hash, shift)
    if (bitmap.and(bit) == 0) return this

    idx := index(bit)
    keyOrNull := list[2*idx]
    valOrNode := list[2*idx+1]

    if (keyOrNull == null)
    {
      RNode? n := ((RNode) valOrNode).remove(shift+5, key)
      if (n == valOrNode) return this
      if (n != null)      return BitmapIndexedNode(bitmap, dupSet(list, 2*idx+1, n))
      if (bitmap == bit)  return null
      return BitmapIndexedNode(bitmap.xor(bit), remPair(list, idx))
    }

    if (key == keyOrNull)
      // TODO: collapse
      return BitmapIndexedNode(bitmap.xor(bit), remPair(list, idx))

    return this
  }

  ** Each impl for BitmapIndexedNode
  override Void each(|Rec| f)
  {
    i := 0
    while (i < list.size)
    {
      key       := list[i]
      nodeOrVal := list[i+1]
      i += 2
      if (key != null)
      {
        f(nodeOrVal)
      }
      else if(nodeOrVal != null)
      {
        ((RNode)nodeOrVal).each(f)
      }
    }
  }

  ** Create a new indexed node with given 2 key/val pairs.
  private RNode createNode(Int shift, Int key1, Rec val1, Int key2, Rec val2)
  {
    key1hash := key1.hash
    key2hash := key2.hash

    // TODO: not possible to have hash collision on Ints; but keep
    // this code in here in case we port to a generic version
    if (key1.hash == key2.hash) throw Err("HashCollision? $key1 ?= $key2")
    //  return HashCollisionNode(null, key1hash, 2, new Object[] {key1, val1, key2, val2})

    addedLeaf := Box(null)
    return BitmapIndexedNode.empty
      .set(shift, key1, val1, addedLeaf)
      .set(shift, key2, val2, addedLeaf)
  }

  ** TODO
  private Int bitpos(Int hash, Int shift)
  {
    1.shiftl(mask(hash, shift))
  }

  ** The index is the number of 1's to the right of 'bit' in 'bitmap'
  private Int index(Int bit)
  {
    bitCount(bitmap.and(bit-1))
  }

  ** Return the bit population count for given integer 'i'.
  private Int bitCount(Int i)
  {
    i = i - i.shiftr(1).and(0x55555555)
    i = i.and(0x33333333) + i.shiftr(2).and(0x33333333)
    i = (i + i.shiftr(4)).and(0x0f0f0f0f)
    i = i + i.shiftr(8)
    i = i + i.shiftr(16)
    return i.and(0x3f)
  }

  private const Int bitmap
  private const Obj?[] list
}