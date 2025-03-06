from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from os.path import join, abspath
from typing import Optional, Generator, Iterable, List, Any, Dict

import pytest
from fs import open_fs
from fs.base import FS
from fs.copy import copy_fs
from fs.errors import ResourceNotFound
from fs.osfs import OSFS
from fs.subfs import SubFS

from relic.sga.v2.serialization import RelicDateTimeSerializer

logger = logging.getLogger(__name__)


def get_data_path(*parts: str) -> str:
    data = abspath(join(__file__, "..", "..", "data"))
    return join(data, *parts)


def get_dataset_path(*parts: str) -> str:
    return get_data_path("dataset", *parts)


def get_datasets() -> List[str]:
    dataset_root = get_dataset_path()
    children = os.listdir(dataset_root)
    datasets = [join(dataset_root, child) for child in children]
    return datasets


@contextmanager
def create_temp_dataset_fs(
    path: str, identifier: Optional[str] = None
) -> Generator[FS, None, None]:
    with open_fs(f"temp://{identifier or ''}") as tmp:
        # Copy files into tmp filesytem
        copy_fs(path, tmp, preserve_time=True)

        # Fix arciv absolute paths
        for match in tmp.glob("**/*.arciv"):
            match_path: str = match.path
            arciv_txt = tmp.readtext(match_path)
            arciv_txt = arciv_txt.replace("<cwd>", tmp.getsyspath("/"))
            tmp.writetext(match_path, arciv_txt)

        yield tmp


@dataclass
class ManifestFileInfo:
    modified: datetime | None = None
    crc: int | None = None
    drive: str | None = None
    archive_path: str | None = None

    @classmethod
    def parse(cls, **kwargs: Any) -> ManifestFileInfo:
        _MODIFIED = "modified"

        if _MODIFIED in kwargs:
            kwargs[_MODIFIED] = RelicDateTimeSerializer.unix2datetime(kwargs[_MODIFIED])

        return cls(**kwargs)


@dataclass
class Manifest:
    toc: Dict[str, Dict[str, ManifestFileInfo]]

    @classmethod
    def parse(cls, **kwargs: Any) -> Manifest:
        _TOC = "toc"
        _FILES = "files"

        if _FILES in kwargs:
            logger.critical(
                "Manifest tried parsing an outdated manifest, root was 'files' instead of 'toc'!"
            )
            kwargs[_TOC] = kwargs[_FILES]
            del kwargs[_FILES]

        if _TOC in kwargs:
            kwargs[_TOC] = {
                drive: {
                    path: ManifestFileInfo.parse(**info)
                    for path, info in file_manifest.items()
                }
                for drive, file_manifest in kwargs[_TOC].items()
            }

        return cls(**kwargs)


def load_manifest(dataset: str, manifest: str) -> Manifest | None:
    with OSFS(dataset) as fs:
        try:
            with fs.opendir("Meta") as meta_dir:
                if not meta_dir.exists(manifest):
                    return None
                data = json.loads(meta_dir.gettext(manifest))
                return Manifest.parse(**data)
        except ResourceNotFound:
            return None


@contextmanager
def safe_open_folder(fs: FS, folder_name: str) -> SubFS[FS]:
    if not fs.exists(folder_name):
        pytest.skip(f"'{folder_name}' folder was not found; skipping validation")

    with fs.opendir(folder_name) as dir:
        yield dir
