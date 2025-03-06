# Relic Tool - SGA - V2
[![PyPI](https://img.shields.io/pypi/v/relic-tool-sga-v2)](https://pypi.org/project/relic-tool-sga-v2/)
[![PyPI - Python Version](https://img.shields.io/pypi/v/relic-tool-sga-v2)](https://www.python.org/downloads/)
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
See https://github.com/MAK-Relic-Tool/SGA-Core

This plugin allows SGA-Core to properly unpack SGA-V2 files.

Please refer to the SGA-Core documentation for information on the unpack command


### Packing Archives
Converts a `.arciv`-like file into a `.sga` archive file.
```
relic sga pack v2 manifest [out_path]
```
```
usage: relic sga pack v2 [-h] [--log [LOG]] [--loglevel [{none,debug,info,warning,error,critical}]] [--logconfig [LOGCONFIG]] manifest [out_path]

positional arguments:
  manifest              An .arciv file (or a suitable .json matching the .arciv tree). If the file extension is not '.json' or '.arciv', '.arciv'
                        is assumed
  out_path              The path to the output SGA file. If the path is a directory, the SGA will be placed in the directory using the name
                        specified in the manifest. If not specified, defaults to the manifest's directory.

options:
  -h, --help            show this help message and exit
  --log [LOG]           Path to the log file, if one is generated
  --loglevel [{none,debug,info,warning,error,critical}]
                        Verbosity of the log. Defaults to `info`
  --logconfig [LOGCONFIG]
                        Path to a logging config file.
```
### Re-Packing Archives
Unpacks and repacks an `.sga` archive.

Primarily useful for testing, or adding missing file headers.

If the official archive viewer fails to find/verify crc32 checksums, this may help.

```
relic sga repack v2 in_sga [out_sga]
```
```
usage: relic sga repack v2 [-h] [--log [LOG]] [--loglevel [{none,debug,info,warning,error,critical}]] [--logconfig [LOGCONFIG]] in_sga [out_sga]

positional arguments:
  in_sga                Input SGA File
  out_sga               Output SGA File

options:
  -h, --help            show this help message and exit
  --log [LOG]           Path to the log file, if one is generated
  --loglevel [{none,debug,info,warning,error,critical}]
                        Verbosity of the log. Defaults to `info`
  --logconfig [LOGCONFIG]
                        Path to a logging config file.
```



## Report A Bug / Issue
Visit the [Issue Tracker](https://github.com/MAK-Relic-Tool/Issue-Tracker/issues)
