# SleuthKit Bundled Binaries

This directory contains bundled SleuthKit binaries for supported platforms.

Expected layout:

```
vendor/sleuthkit/
  linux-x86_64/
    fls
    mmls
    icat (optional)
  win64/
    fls.exe
    mmls.exe
    icat.exe (optional)
  LICENSE-CPL-1.0.txt
  NOTICE.txt
```

Place the appropriate binaries in the platform subdirectories before building release
artifacts. See `planning/wip/bundle_sleuthkit.md` for acquisition details.
