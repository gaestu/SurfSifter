# Third-Party Tools & Licenses

| Tool | Purpose | License | Bundled |
| ---- | ------- | ------- | ------- |
| bulk_extractor | URL discovery, feature extraction | GPLv3 | No – discovered at runtime |
| foremost | Data carving | GPLv2 | No – discovered at runtime |
| scalpel | Data carving (alternative) | GPLv2 | No – discovered at runtime |
| exiftool | Metadata extraction | Artistic License 2.0 | No – discovered at runtime |
| ewfmount / libewf | EWF mounting | LGPL-2.1 | No – discovered at runtime |
| sleuthkit (fls, mmls, icat) | File system listing & inode extraction | CPL-1.0 | Yes – bundled for supported platforms |
| pytsk3 | NTFS parsing | CPL / IPL | Dependency |

The core project is licensed under Apache-2.0. The GUI never vendors GPL binaries; it reads configuration to locate those tools on the examiner workstation. SleuthKit is bundled under CPL-1.0 where applicable.
