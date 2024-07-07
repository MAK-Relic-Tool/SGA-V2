import json
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from logging.config import fileConfig
from logging.handlers import RotatingFileHandler
from os.path import splitext
from pathlib import Path
from typing import Optional, Dict, Any
import logging

from relic.core.cli import CliPlugin, _SubParsersAction, RelicArgParser
from relic.core.errors import RelicToolError
from relic.sga.core.cli import _get_file_type_validator

from relic.sga.v2 import arciv
from relic.sga.v2.arciv import Arciv
from relic.sga.v2.essencefs.definitions import SgaFsV2Packer, EssenceFSV2
from relic.sga.v2.serialization import SgaV2GameFormat

_CHUNK_SIZE = 1024 * 1024 * 4  # 4 MiB

LOGLEVEL_TABLE = {
    "none": logging.NOTSET,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


@dataclass
class LogingOptions:
    log_file: Optional[str]
    log_level: int
    log_config: Optional[str]


def _add_logging_to_parser(
    parser: ArgumentParser,
) -> None:
    """Adds [-l --log] and [-ll --loglevel] commands."""
    parser.add_argument(
        "--log",
        type=_get_file_type_validator(False),
        help="Path to the log file, if one is generated",
        nargs="?",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--loglevel",
        help="Verbosity of the log. Defaults to `info`",
        nargs="?",
        required=False,
        default="info",
        choices=list(LOGLEVEL_TABLE.keys()),
    )
    parser.add_argument(
        "--logconfig",
        type=_get_file_type_validator(True),
        help="Path to a logging config file.",
        nargs="?",
        required=False,
    )


def _add_logging_to_command(ns: Namespace):
    logger = logging.getLogger()
    options = _extract_logging_from_namespace(ns)
    setup_logging_for_cli(options, logger=logger)
    return logger


def _extract_logging_from_namespace(ns: Namespace) -> LogingOptions:
    log_file: Optional[str] = ns.log
    log_level_name: str = ns.loglevel
    log_level = LOGLEVEL_TABLE[log_level_name]
    log_config: Optional[str] = ns.logconfig
    return LogingOptions(log_file, log_level, log_config)


def _create_log_formatter():
    return logging.Formatter(
        fmt="%(levelname)s:%(name)s::%(filename)s:L%(lineno)d:\t%(message)s (%(asctime)s)",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _create_file_handler(log_file: str, log_level: int):
    f = _create_log_formatter()
    h = RotatingFileHandler(
        log_file,
        encoding="utf8",
        maxBytes=100000,
        backupCount=-1,
    )
    h.setFormatter(f)
    h.setLevel(log_level)
    return h


def _create_console_handlers(
    log_level: int, err_level: Optional[int] = logging.WARNING
):
    f_out = logging.Formatter("%(message)s")
    f_err = _create_log_formatter()

    h_out = logging.StreamHandler(sys.stdout)
    h_err = logging.StreamHandler(sys.stderr)

    h_out.setFormatter(f_out)
    h_err.setFormatter(f_err)

    # def filter(record):
    #     if record.levelno >= err_level:
    #         return 0
    #     else:
    #         return 1
    # h_out.addFilter(filter)
    h_out.addFilter(lambda record: 0 if record.levelno >= err_level else 1)
    h_err.addFilter(lambda record: 0 if record.levelno < err_level else 1)

    h_out.setLevel(log_level)
    h_err.setLevel(max(err_level, log_level))
    return h_out, h_err


def setup_logging_for_cli(
    options: LogingOptions,
    print_log: bool = True,
    logger: Optional[logging.Logger] = None,
):
    logger = logger or logging.getLogger()  # Root logger
    # Run first to ovveride other loggers
    if options.log_config is not None:
        fileConfig(options.log_config)

    logger.setLevel(options.log_level)

    if options.log_file is not None:
        h_log_file = _create_file_handler(options.log_file, options.log_level)
        logger.addHandler(h_log_file)

    if print_log:
        h_out, h_err = _create_console_handlers(options.log_level, logging.WARNING)
        logger.addHandler(h_out)
        logger.addHandler(h_err)


# if __name__ == "__main__":
#     p = ArgumentParser()
#     _add_logging_to_parser(p)
#     ns = p.parse_args(["--log", "blah.log", "--loglevel", "debug"])
#     log_opt = _extract_logging_from_namespace(ns)
#     setup_logging_for_cli(log_opt)
#     logger = logging.getLogger()
#     logger.debug("Debug")
#     logger.info("Info")
#     logger.warning("Warning")
#     logger.error("Error")
#     logger.critical("Ciritcal")
#
#     p.print_help()
#     exit()


class RelicSgaPackV2Cli(CliPlugin):
    def _create_parser(
        self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = RelicArgParser("v2")
        else:
            parser = command_group.add_parser("v2")

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
        _add_logging_to_parser(parser)
        return parser

    def command(self, ns: Namespace) -> Optional[int]:
        logger = _add_logging_to_command(ns)
        # Extract Args
        manifest_path: str = ns.manifest
        out_path: str = ns.out_path
        file_name: str = None  # type: ignore

        manifest_is_json = splitext(manifest_path)[1].lower() == ".json"

        def _check_parts(_path: str) -> bool:
            if not _path:  # If empty, assume we're done
                return True

            d, f = os.path.split(_path)

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
        logger.info(f"\tReading Manifest `{manifest_path}`")
        with open(manifest_path, "r") as manifest_handle:
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
        logger.info(f"\tPacking SGA `{full_out_path}`")
        with open(full_out_path, "wb") as out_handle:
            SgaFsV2Packer.pack(manifest, out_handle, safe_mode=True)
        logger.info("\t\tPacked")
        logger.info("\tDone!")
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

        _add_logging_to_parser(parser)
        return parser

    def command(self, ns: Namespace) -> Optional[int]:
        logger = _add_logging_to_command(ns)
        # Extract Args
        in_sga: str = ns.in_sga
        out_sga: str = ns.out_sga

        # Execute Command

        if out_sga is None:
            logger.info(f"Re-Packing `{in_sga}`")
        else:
            logger.info(f"Re-Packing `{in_sga}` as `{out_sga}`")

        # Create 'SGA'
        logger.info(f"\tReading `{in_sga}`")
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
                logger.info(f"\tWriting `{out_sga}`")

                with open(out_sga, "wb") as sga_file:
                    sgafs.save(sga_file)
            else:
                sgafs.save()
            logger.info(f"\tDone!")

        return None


class RelicSgaV2Legacy2ArcivCli(CliPlugin):
    def _create_parser(
        self, command_group: Optional[_SubParsersAction] = None
    ) -> ArgumentParser:
        parser: ArgumentParser
        if command_group is None:
            parser = RelicArgParser("v2")
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

        _add_logging_to_parser(parser)
        return parser

    def command(self, ns: Namespace) -> Optional[int]:
        logger = _add_logging_to_command(ns)
        # Extract Args
        in_sga: str = ns.in_sga
        out_sga: str = ns.out_sga

        # Execute Command

        if out_sga is None:
            logger.info(f"Re-Packing `{in_sga}`")
        else:
            logger.info(f"Re-Packing `{in_sga}` as `{out_sga}`")

        # Create 'SGA'
        logger.info(f"\tReading `{in_sga}`")
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
                logger.info(f"\tWriting `{out_sga}`")

                with open(out_sga, "wb") as sga_file:
                    sgafs.save(sga_file)
            else:
                sgafs.save()
            logger.info(f"\tDone!")

        return None
