import json
import os
import typing
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Optional, Dict, Any

import fs
from relic.core.cli import CliPlugin, _SubParsersAction
from relic.sga.core.cli import _get_dir_type_validator, _get_file_type_validator
from relic.sga.core.definitions import StorageType

from relic.sga.v2.serialization import SgaV2GameFormat
from relic.sga.v2.sgafs import SgaFsV2, SgaFsV2Packer

_CHUNK_SIZE = 1024 * 1024 * 4  # 4 MiB


def _resolve_storage_type(s: Optional[str]) -> StorageType:
    _HELPER = {
        "STORE": StorageType.STORE,
        "BUFFER": StorageType.BUFFER_COMPRESS,
        "STREAM": StorageType.STREAM_COMPRESS,
    }
    if s is None:
        return StorageType.STORE

    s = s.upper()
    if s in _HELPER:
        return _HELPER[s]
    else:
        return StorageType[s]


class RelicSgaPackV2Cli(CliPlugin):
    def _create_parser(
            self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = ArgumentParser("v2")
        else:
            parser = command_group.add_parser("v2")

        parser.add_argument(
            "src_dir",
            type=_get_dir_type_validator(exists=True),
            help="Source Directory",
        )
        parser.add_argument(
            "out_sga",
            type=_get_file_type_validator(exists=False),
            help="Output SGA File",
        )
        parser.add_argument(
            "config_file",
            type=_get_file_type_validator(exists=True),
            help="Config .json file",
        )

        return parser

    def command(self, ns: Namespace) -> Optional[int]:
        # Extract Args
        working_dir: str = ns.src_dir
        outfile: str = ns.out_sga
        config_file: str = ns.config_file
        with open(config_file) as json_h:
            config: Dict[str, Any] = json.load(json_h)

        # Execute Command
        print(f"Packing `{outfile}`")

        # Create 'SGA'
        sga = SgaFsV2()
        archive_name = os.path.basename(outfile)
        sga.setmeta(
            {
                "name": archive_name,  # Specify name of archive
                "header_md5": "0"
                              * 16,  # Must be present due to a bug, recalculated when packed
                "file_md5": "0"
                            * 16,  # Must be present due to a bug, recalculated when packed
            },
            "essence",
        )

        # Walk Drives
        for alias, drive in config.items():
            name = drive["name"]

            print(f"\tPacking Drive `{name}` w/ alias `{alias}`")
            sga_drive = None  # sga.create_drive(alias)

            # CWD for drive operations
            drive_cwd = os.path.join(working_dir, drive.get("path", ""))

            # Try to pack files
            print(f"\tScanning files in `{drive_cwd}`")
            frontier = set()
            _R = Path(drive_cwd)

            # Run matchers
            for solver in drive["solvers"]:
                # Determine storage type
                storage = _resolve_storage_type(solver.get("storage"))
                # Find matching files
                for path in _R.rglob(solver["match"]):
                    if not path.is_file():  # Edge case handling
                        continue
                    # File Info ~ Name & Size
                    full_path = str(path)
                    if full_path in frontier:
                        continue
                    path_in_sga = os.path.relpath(full_path, drive_cwd)
                    size = os.stat(full_path).st_size

                    # Dumb way of supporting query
                    query = solver.get("query")
                    if query is None or len(query) == 0:
                        ...  # do nothing
                    else:
                        result = eval(query, {"size": size})
                        if not result:
                            continue  # Query Failure

                    # match found, copy file to FS
                    # EssenceFS is unfortunately,
                    print(
                        f"\t\tPacking File `{os.path.relpath(full_path, drive_cwd)}` w/ `{storage.name}`"
                    )
                    frontier.add(full_path)
                    if (
                            sga_drive is None
                    ):  # Lazily create drive, to avoid empty drives from being created
                        sga_drive = sga.create_drive(alias, name)

                    with open(full_path, "rb") as unpacked_file:
                        parent, file = os.path.split(path_in_sga)
                        with sga_drive.makedirs(parent, recreate=True) as folder:
                            with folder.openbin(file, "w") as packed_file:
                                while True:
                                    buffer = unpacked_file.read(_CHUNK_SIZE)
                                    if len(buffer) == 0:
                                        break
                                    packed_file.write(buffer)
                        sga_drive.setinfo(
                            path_in_sga, {"essence": {"storage_type": storage}}
                        )

        print(f"Writing `{outfile}` to disk")
        # Write to binary file:
        with open(outfile, "wb") as sga_file:
            SgaFsV2Packer.serialize(sga, sga_file)
        print(f"\tDone!")

        return None


class RelicSgaRepackV2Cli(CliPlugin):
    def _create_parser(
            self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = ArgumentParser("v2")
        else:
            parser = command_group.add_parser("v2")

        parser.add_argument(
            "in_sga", type=_get_file_type_validator(exists=True), help="Input SGA File"
        )
        parser.add_argument(
            "out_sga",
            nargs="?",
            type=_get_file_type_validator(exists=False),
            help="Output SGA File",
            default=None,
        )

        return parser

    def command(self, ns: Namespace) -> Optional[int]:
        # Extract Args
        in_sga: str = ns.in_sga
        out_sga: str = ns.out_sga

        # Execute Command

        if out_sga is None:
            print(f"Re-Packing `{in_sga}`")
        else:
            print(f"Re-Packing `{in_sga}` as `{out_sga}`")

        # Create 'SGA'
        print(f"\tReading `{in_sga}`")
        if "Dawn of War" in in_sga:
            game_format = SgaV2GameFormat.DawnOfWar
        elif "Impossible Creatures" in in_sga:
            game_format = SgaV2GameFormat.ImpossibleCreatures
        else:
            game_format = None

        if out_sga is not None:
            Path(out_sga).parent.mkdir(parents=True, exist_ok=True)

        with open(in_sga, "rb") as sga_h:
            sgafs = SgaFsV2(sga_h, parse_handle=True, in_memory=True, game=game_format)
            # Write to binary file:
            if out_sga is not None:
                print(f"\tWriting `{out_sga}`")

                with open(out_sga, "wb") as sga_file:
                    sgafs.save(sga_file)
            else:
                sgafs.save()
            print(f"\tDone!")

        return None


if __name__ == "__main__":
    # shortcut to root cli
    from relic.core.cli import cli_root

    cli_root.run()
