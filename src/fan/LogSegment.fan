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
** LogSegment
**
internal class LogSegment
{
  ** Read an existing closed segment.
  static Void read(File file, Str:Bucket buckets)
  {
    InStream? in
    try
    {
      // verify header
      in = file.in
      if (in.readS8 != magic)   throw IOErr("Invalid magic")
      if (in.read   != version) throw IOErr("Unsupported version")

      buf := Buf(8192)
      Bucket? bucket
      bname  := ""
      id     := 0
      ver    := 0
      tags   := Str:Obj[:]
      strbuf := StrBuf()
      tname  := ""
      Obj? val

      while (in.peek != null)
      {
        buf.clear

        size := in.readU4
        in.readBufFully(buf, size)

        try
        {
          // validate record crc
          crc := in.readU4
          if (buf.crc("CRC-32") != crc) throw IOErr("Invalid CRC")

          // read bucket name
          strbuf.clear
          while (buf.peek != 0) strbuf.addChar(buf.read)
          buf.read
          bname = strbuf.toStr

          // lookup or create bucket
          bucket = buckets[bname]
          if (bucket == null) buckets[bname] = bucket = Bucket(bname)

          // parse rec
          id  = buf.readU4
          ver = buf.readU4
          tags.clear

          // check for tombstone
          if (buf.peek == tombstone)
          {
            bucket.unload(id)
            continue
          }

          while (buf.pos < buf.size)
          {
            // read tag name
            strbuf.clear
            while (buf.peek != 0) strbuf.addChar(buf.read)
            buf.read
            tname = strbuf.toStr

            // read tag val
            tc := buf.read
            switch (tc)
            {
              case tcBool:  val = buf.read == 1
              case tcInt:   val = buf.readS8
              case tcFloat: val = buf.readF8
              case tcStr:
                strbuf.clear
                while (buf.peek != 0) strbuf.addChar(buf.readChar)
                buf.read
                val = strbuf.toStr
              case tcUri:
                strbuf.clear
                while (buf.peek != 0) strbuf.addChar(buf.readChar)
                buf.read
                val = strbuf.toStr.toUri
              case tcBuf:
                len := buf.readU2
                val = buf.readBufFully(null, len).trim
              case tcDate:
                val = Date(buf.readU2+2000, Month.vals[buf.read], buf.read)
              case tcTime:
                val = Time(buf.read, buf.read, buf.read)
              case tcDT:
                val = DateTime.makeTicks(buf.readS8, TimeZone.utc)
              default: throw ArgErr("Unknown type '0x${tc.toHex(2)}'")
            }

            tags[tname.toStr] = val
          }

          rec := Rec(id, ver, tags)
          bucket.load(rec)
        }
        catch (Err err)
        {
          // TODO: how do we handle local rec failures?
          err.trace
        }
      }
    }
    finally { in?.close }
  }

  ** Construct a new active Segment for writing.
  new make(File file)
  {
    this.file = file
  }

  ** Get segment index, which is the file ext of backing file.
  Int index() { file.ext.toInt }

  ** Open this segment.
  This open()
  {
    if (out != null) throw IOErr("Segment already open")
    if (file.exists) throw IOErr("Segment already exists")

    // TODO: lock file -- see notes/notes.md

    // load and write segment header
    this.out = file.out
    out.writeI8(magic).write(version).flush.sync
    return this
  }

  ** Close this segment.
  This close()
  {
    this.sync
    this.out?.close
    this.out = null
    return this
  }

  ** Append the given record to this segment. Note that this method
  ** leaves buffered content in OutStream so caller should be aware
  ** if and when a `sync` is required.
  This append(Str bucket, Rec rec, Bool rem := false)
  {
    buf.clear
    buf.print(bucket).write(0x00)
    buf.writeI4(rec.id)
    buf.writeI4(rec.ver)

    if (rem == false)
    {
      // write record add/update

      // TODO -- boolean compressing -- couple with type code: 0x10=false; 0x11=true
      // TODO -- int compressio? 1/2/4 bytes on length?  sign bit?
      // TODO -- string compression?
      //   gzip > 128:
      //   smaz < 128: https://github.com/antirez/smaz
      // TODO -- buf len sizing / u2? u4?

      rec.tags.each |val, name|
      {
        buf.print(name).write(0x00)
        switch (val.typeof)
        {
          case Bool#:  buf.write(tcBool).write(val == true ? 1 : 0)
          case Int#:   buf.write(tcInt).writeI8(val)
          case Float#: buf.write(tcFloat).writeF8(val)
          case Str#:   buf.write(tcStr).print(val).write(0x00)
          case Uri#:   buf.write(tcUri).print(val).write(0x00)
          case ftBuf:
            Buf b := val
            buf.write(tcBuf).writeI2(b.size).writeBuf(b)
          case Date#:
            Date d := val
            buf.write(tcDate)
            buf.writeI2(d.year-2000).write(d.month.ordinal).write(d.day)
          case Time#:
            Time t := val
            buf.write(tcTime)
            buf.write(t.hour).write(t.min).write(t.sec)
          case DateTime#:
            DateTime ts := val
            buf.write(tcDT)
            buf.writeI8(ts.toUtc.ticks)
          default: throw ArgErr("Value type '$val.typeof' not supported")
        }
      }
    }
    else
    {
      // remove marked with tombstone
      buf.write(tombstone)
    }

    // TODO: check if buf.size > max_file_size
    // TODO: check if out of disk space

    crc := buf.crc("CRC-32")
    out.writeI4(buf.size).writeBuf(buf.seek(0)).writeI4(crc)
    return this
  }

  ** Flush any outstanding writes to disk.
  This sync()
  {
    if (out != null) out.flush.sync
    return this
  }

  private static const Int magic   := 0x4252_554E_4F4C_4F47   // BRUNOLOG
  private static const Int version := 0x01

  private static const Int tcBool  := 0x01
  private static const Int tcInt   := 0x02
  private static const Int tcFloat := 0x03
  private static const Int tcStr   := 0x04
  private static const Int tcUri   := 0x05
  private static const Int tcBuf   := 0x06
  private static const Int tcDate  := 0x07
  private static const Int tcTime  := 0x08
  private static const Int tcDT    := 0x09

  private static const Type ftBuf := Type.find("sys::ConstBuf")

  private static const Int tombstone := 0x7f  // ASCII DEL

  private const File file
  private OutStream? out
  private Buf buf := Buf { it.capacity=8192 }
}