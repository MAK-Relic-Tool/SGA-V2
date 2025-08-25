# Relic Tool - SGA - V2
[![PyPI](https://img.shields.io/pypi/v/relic-tool-sga-v2)](https://pypi.org/project/relic-tool-sga-v2/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/relic-tool-sga-v2)](https://www.python.org/downloads/)
[![PyPI - License](https://img.shields.io/pypi/l/relic-tool-sga-v2)](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/blob/main/LICENSE.txt)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Pytest](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/pytest.yml/badge.svg)](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/pytest.yml)
[![Pylint](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/pylint.yml/badge.svg)](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/pylint.yml)
[![MyPy](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/mypy.yml/badge.svg)](https://github.com/MAK-Relic-Tool/Relic-Tool-SGA-V2/actions/workflows/mypy.yml)
#### Disclaimer
Not affiliated with Sega, Relic Entertainment, or THQ.
#### Description
A plugin to read/write Relic SGA (V2) archive files.
#### Game Support
A non-exhaustive list of Games that use Relic's SGA V2
- Dawn Of War Gold
- Dawn Of War: Winter Assault
- Dawn Of War: Dark Crusade
- Dawn Of War: Soulstorm
- Impossible Creatures
- Dawn Of War: Anniversary Edition
- Dawn Of War: Definitive Edition
#### File Format Schema
Check out the [Schema Docs](https://mak-relic-tool.github.io/Schema-Documentation/)

## Installation (Pip)
### Installing from PyPI (Recommended)
```
pip install relic-tool-sga-v2
```
### Installing from GitHub
For more information, see [pip VCS support](https://pip.pypa.io/en/stable/topics/vcs-support/#git)
```
pip install git+https://github.com/MAK-Relic-Tool/SGA-V2
```
## CLI Commands
### Unpacking Archives
This plugin allows SGA-Core to properly unpack SGA-V2 files.

### Packing Archives
Converts a `.arciv`-like file into a `.sga` archive file.
```
relic sga v2 pack manifest [out_path]
```
```
usage: relic sga v2 pack [-h] [--log [LOG]] [--loglevel [{none,debug,info,warning,error,critical}]] [--logconfig [LOGCONFIG]] manifest [out_path]

positional arguments:
  manifest              An .arciv file (or a suitable .json matching the .arciv tree). If the file extension is not '.json' or '.arciv', '.arciv' is assumed
  out_path              The path to the output SGA file. If the path is a directory, the SGA will be placed in the directory using the name specified in the manifest. If not specified, defaults to the manifest's directory.

options:
  -h, --help            show this help message and exit
  --log [LOG]           Path to the log file, if one is generated
  --loglevel [{none,debug,info,warning,error,critical}]
                        Verbosity of the log. Defaults to `info`
  --logconfig [LOGCONFIG]
                        Path to a logging config file.
```
### Unpacking Archives
Unpacks an `.sga` archive into a file tree.
```
relic sga unpack src_sga out_dir
```
```
usage: relic sga unpack [-h] [-m | -i] [--log [LOG]] [--loglevel [{none,debug,info,warning,error,critical}]] [--logconfig [LOGCONFIG]] src_sga out_dir

Unpack an SGA archive to the filesystem. If only one root is present in the SGA, '--merge' is implied. If multiple roots are in the SGA '--isolate' is implied. Manually specify the flags to override this behaviour.

positional arguments:
  src_sga               Source SGA File
  out_dir               Output Directory

options:
  -h, --help            show this help message and exit
  -m, --merge           SGA roots will always write to the same folder; specified by out_dir
  -i, --isolate         SGA roots will always write to separate folders, one per alias; located within out_dir
  --log [LOG]           Path to the log file, if one is generated
  --loglevel [{none,debug,info,warning,error,critical}]
                        Verbosity of the log. Defaults to `info`
  --logconfig [LOGCONFIG]
                        Path to a logging config file.
```
### Verifying Archives
Checks the MD5 hashes and/or the SGA's file block CRCs

Useful for verifying if file's have been modified or corrupted.

SGA archives may still be used in-game even if they fail verification
```
relic sga v2 verify sga_file
```
```
usage: relic sga v2 verify [-h] [--all] [-H] [-D] [-F] [-q] [-e] [--tree] [--log [LOG]] [--loglevel [{none,debug,info,warning,error,critical}]] [--logconfig [LOGCONFIG]] sga_file

positional arguments:
  sga_file                   Input SGA File

options:
  -h, --help            show this help message and exit
  --all                 Shorthand for '-H -D -F', if no flags are specified, '--all' is implied
  -H, --header          Verify the SGA Header's MD5 hash
  -D, --data            Verify SGA Data's MD5 hash
  -F, --files           Verify SGA Files's CRC32 hashes
  -q, --quiet           Only print failures and errors
  -e, --error           Stop on first failure or error
  --tree                Prints the SGA File's results in a tree format, if '-q' is specified, folders will still be printed
  --log [LOG]           Path to the log file, if one is generated
  --loglevel [{none,debug,info,warning,error,critical}]
                        Verbosity of the log. Defaults to `info`
  --logconfig [LOGCONFIG]
                        Path to a logging config file.

```


## Report A Bug / Issue
Visit the [Issue Tracker](https://github.com/MAK-Relic-Tool/Issue-Tracker/issues)
