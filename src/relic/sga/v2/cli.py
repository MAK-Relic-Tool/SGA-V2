import json
import logging
import os
from argparse import ArgumentParser, Namespace
from os.path import splitext
from pathlib import Path
from typing import Optional, Dict, Any

from relic.core.cli import CliPlugin, _SubParsersAction, RelicArgParser, CliPluginGroup
from relic.core.errors import RelicToolError
from relic.sga.core.cli import _get_file_type_validator

from relic.sga.v2 import arciv
from relic.sga.v2.arciv import Arciv
from relic.sga.v2.essencefs.definitions import SgaFsV2Packer, EssenceFSV2
from relic.sga.v2.serialization import SgaV2GameFormat
from relic.core.logmsg import BraceMessage

_CHUNK_SIZE = 1024 * 1024 * 4  # 4 MiB


class RelicSgaV2Cli(CliPluginGroup):
    GROUP = "relic.cli.sga.v2"

    def _create_parser(
        self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        name = "v2"
        if command_group is None:
            return RelicArgParser(name)
        return command_group.add_parser(name)


class RelicSgaPackV2Cli(CliPlugin):
    def _create_parser(
        self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = RelicArgParser("pack")
        else:
            parser = command_group.add_parser("pack")

        parser.add_argument(
            "manifest",
            type=_get_file_type_validator(exists=True),
            help="An .arciv file (or a suitable .json matching the .arciv tree)."
            " If the file extension is not '.json' or '.arciv', '.arciv' is assumed",
        )
        parser.add_argument(
            "out_path",
            type=str,
            help="The path to the output SGA file."
            " If the path is a directory, the SGA will be placed in the directory using the name specified in the manifest."
            " If not specified, defaults to the manifest's directory.",
            # required=False,
            nargs="?",
            default=None,
        )
        return parser

    def command(self, ns: Namespace, *, logger: logging.Logger) -> Optional[int]:
        # Extract Args
        manifest_path: str = ns.manifest
        out_path: str = ns.out_path
        file_name: str = None  # type: ignore

        manifest_is_json = splitext(manifest_path)[1].lower() == ".json"

        def _check_parts(_path: str) -> bool:
            if not _path:  # If empty, assume we're done
                return True

            d, _ = os.path.split(_path)

            if os.path.exists(d):
                return not os.path.isfile(d)
            if (
                _path == d
            ):  # If, somehow, we try to recurse into ourselves, assume we're done
                return True
            return _check_parts(d)

        if out_path is None:
            out_path = os.path.dirname(manifest_path)
        elif os.path.exists(out_path):
            if os.path.isdir(out_path):
                ...
                # Do nothing to out path
            else:
                out_path, file_name = os.path.split(out_path)
        elif not _check_parts(out_path):
            raise RelicToolError(
                f"'{out_path}' is not a valid path; it treats a file as a directory!"
            )
        else:
            out_path, file_name = os.path.split(out_path)

        # Execute Command
        logger.info("SGA Packer")
        logger.info(
            BraceMessage(
                "\tReading Manifest `{manifest_path}`", manifest_path=manifest_path
            )
        )
        with open(manifest_path, "r", encoding="utf-8") as manifest_handle:
            if manifest_is_json:
                manifest_json: Dict[str, Any] = json.load(manifest_handle)
                manifest = Arciv.from_parser(manifest_json)
            else:
                manifest = arciv.load(manifest_handle)
        logger.info("\t\tLoaded")

        # Resolve name when out_path was passed in as a directory
        if file_name is None:
            file_name = manifest.ArchiveHeader.ArchiveName + ".sga"
        # Create parent directories
        if out_path != "":  # Local path will lack out_path
            os.makedirs(out_path, exist_ok=True)
        # Create full path
        full_out_path = os.path.join(out_path, file_name)
        logger.info(
            BraceMessage("\tPacking SGA `{full_out_path}`", full_out_path=full_out_path)
        )
        with open(full_out_path, "wb") as out_handle:
            SgaFsV2Packer.pack(manifest, out_handle, safe_mode=True)
        logger.info("\t\tPacked")
        logger.info("\tDone!")
        return 0


class RelicSgaRepackV2Cli(CliPlugin):
    def _create_parser(
        self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = ArgumentParser("repack")
        else:
            parser = command_group.add_parser("repack")

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

    def command(self, ns: Namespace, *, logger: logging.Logger) -> Optional[int]:
        # Extract Args
        in_sga: str = ns.in_sga
        out_sga: str = ns.out_sga

        # Execute Command

        if out_sga is None:
            logger.info(BraceMessage("Re-Packing `{in_sga}`", in_sga=in_sga))
        else:
            logger.info(
                BraceMessage(
                    "Re-Packing `{in_sga}` as `{out_sga}`",
                    in_sga=in_sga,
                    out_sga=out_sga,
                )
            )

        # Create 'SGA'
        logger.info(BraceMessage("\tReading `{in_sga}`", in_sga=in_sga))
        if "Dawn of War" in in_sga:
            game_format = SgaV2GameFormat.DawnOfWar
        elif "Impossible Creatures" in in_sga:
            game_format = SgaV2GameFormat.ImpossibleCreatures
        else:
            game_format = None

        if out_sga is not None:
            Path(out_sga).parent.mkdir(parents=True, exist_ok=True)

        with open(in_sga, "rb") as sga_h:
            sgafs = EssenceFSV2(
                sga_h, parse_handle=True, in_memory=True, game=game_format
            )
            # Write to binary file:
            if out_sga is not None:
                logger.info(BraceMessage("\tWriting `{out_sga}`", out_sga=out_sga))

                with open(out_sga, "wb") as sga_file:
                    sgafs.save(sga_file)
            else:
                sgafs.save()
            logger.info("\tDone!")
        return 0
