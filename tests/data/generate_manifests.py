import json
import logging
import os

from relic.sga.core.hashtools import crc32

logger = logging.getLogger()

_FILE_MANIFEST = "Meta\\manifest.json"
_CONTENT_ROOT = "Root"


def _crc(path: str) -> int:
    with open(path, "rb") as h:
        return crc32(h)


def _mtime(path: str) -> int:
    return int(os.stat(path).st_mtime)


def _full_path(dataset: str, rel_content: str, file: str):
    return os.path.join(dataset, rel_content, file)


def run_dataset(dataset: str, rel_manifest: str, rel_content: str):
    man_fpath = os.path.join(dataset, rel_manifest)
    try:
        with open(man_fpath, "r") as h:
            data = json.load(h)
    except Exception as e:
        logger.error(f"Failed to read Manifest")
        raise

    try:
        for file, meta in data.get("files", {}).items():
            fpath = _full_path(dataset, rel_content, file)
            if "crc" in meta:
                meta["crc"] = _crc(fpath)
            if "modified" in meta:
                meta["modified"] = _mtime(fpath)
    except Exception as e:
        logger.error(f"Failed to parse files")
        raise

    try:
        with open(man_fpath, "w") as h:
            json.dump(data, h)
    except Exception as e:
        logger.error(f"Failed to write Manifest")
        raise


def run(
    dataset_dir: str,
    rel_manifest: str = _FILE_MANIFEST,
    rel_content: str = _CONTENT_ROOT,
):
    for path in os.listdir(dataset_dir):
        fpath = os.path.join(dataset_dir, path)
        if not os.path.isdir(fpath):
            continue
        try:
            logger.info(fpath)
            run_dataset(fpath, rel_manifest, rel_content)
        except Exception as e:
            logger.error(e)
            continue


if __name__ == "__main__":
    _dpath = os.path.abspath(os.path.join(__file__, "..", "dataset"))
    run(_dpath)
