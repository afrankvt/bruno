#! /usr/bin/env fan
//
// Copyright (c) 2017, Andy Frank
// Licensed under the MIT License
//
// History:
//   16 Jun 2017  Andy Frank  Creation
//

using build

**
** Build: bruno
**
class Build : BuildPod
{
  new make()
  {
    podName = "bruno"
    summary = "BrunoDB key-value store"
    version = Version("0.1")
    depends = ["sys 1.0", "util 1.0", "concurrent 1.0",]
    srcDirs = [`fan/`, `test/`]
  }
}