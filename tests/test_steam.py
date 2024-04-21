import os
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Tuple, Set

import fs
import pytest
from fs.info import Info
from fs.walk import Step
from relic.core.lazyio import read_chunks

from relic.sga.v2.serialization import SgaV2GameFormat
from relic.sga.v2.essencefs.definitions import EssenceFSV2

_DOW_DC = "Dawn of War Dark Crusade"
_DOW_GOLD = "Dawn of War Gold"
_DOW_SS = "Dawn of War Soulstorm"
_IMP_CREATURES = "Impossible Creatures"

_ALLOWED_GAMES = [
    _DOW_DC,
    _DOW_GOLD,  # Winter Assault is part of 'Gold' on steam
    _DOW_SS,
    _IMP_CREATURES,
]

games_path = os.environ.get("GAMES")
if games_path is None:
    pytest.skip("GAMES not specified in ENV", allow_module_level=True)

_root = Path(games_path)
_installed_all: Dict[str, List[str]] = {}
_installed_unique: Dict[str, List[str]] = {}
_installed_allowed: dict[str, List[str]] = {}
_unique_sgas: Set[Tuple[str, int]] = set()
_KiB = 1024
_MiB = 1024 * _KiB
_GiB = 1024 * _MiB
_MAX_SGA_TEST_SIZE = (
    8 * _MiB
)  # Some tests are REALLY long on large files; while it would be nice to test them, we test on a subset of the smaller ones to test faster

for game in _ALLOWED_GAMES:
    game_list = _installed_all[game] = []
    unique_list = _installed_unique[game] = []
    allowed_list = _installed_allowed[game] = []
    for sga in (_root / game).rglob("*.sga"):
        game_list.append(str(sga))
        sga_name = sga.relative_to(_root)
        sga_size = sga.stat().st_size
        u_key = (str(sga_name), sga_size)
        unique = u_key not in _unique_sgas
        if unique:
            unique_list.append(str(sga))

            if sga_size <= _MAX_SGA_TEST_SIZE:
                allowed_list.append(str(sga))

_dow_dc_sgas = (
    _installed_all[_DOW_DC],
    _installed_unique[_DOW_DC],
    _installed_allowed[_DOW_DC],
)
_dow_gold_sgas = (
    _installed_all[_DOW_GOLD],
    _installed_unique[_DOW_GOLD],
    _installed_allowed[_DOW_GOLD],
)
_dow_ss_sgas = (
    _installed_all[_DOW_SS],
    _installed_unique[_DOW_SS],
    _installed_allowed[_DOW_SS],
)
_imp_creatures_sgas = (
    _installed_all[_IMP_CREATURES],
    _installed_unique[_IMP_CREATURES],
    _installed_allowed[_IMP_CREATURES],
)

QUICK = False  # skips slower tests like file MD5 checksums and file CRC checks


@contextmanager
def _open_sga(path: str, **kwargs) -> EssenceFSV2:
    game_format: SgaV2GameFormat = None
    if "Dawn of War" in path:
        game_format = SgaV2GameFormat.DawnOfWar
    elif "Impossible Creatures" in path:
        game_format = SgaV2GameFormat.ImpossibleCreatures

    with open(path, "rb") as h:
        yield EssenceFSV2(h, parse_handle=True, game=game_format, **kwargs)


class GameTests:
    def test_open_fs(self, path: str):
        with fs.open_fs(f"sga://{path}"):
            ...

    @pytest.mark.skipif(QUICK, reason="Quick mode, skipping slow tests")
    def test_verify_header(self, path: str):
        with _open_sga(path, verify_header=True, in_memory=False):
            ...

    @pytest.mark.skipif(QUICK, reason="Quick mode, skipping slow tests")
    def test_verify_file(self, path: str):
        with _open_sga(path, verify_file=True, in_memory=False):
            ...

    @pytest.mark.skipif(QUICK, reason="Quick mode, skipping slow tests")
    def test_verify_crc32(self, path: str):
        with _open_sga(path) as sga:
            for file in sga.walk.files():
                result = sga.verify_file_crc(file)
                assert result is True, file

    def test_repack(self, path: str):
        game_format: SgaV2GameFormat = None
        if "Dawn of War" in path:
            game_format = SgaV2GameFormat.DawnOfWar
        elif "Impossible Creatures" in path:
            game_format = SgaV2GameFormat.ImpossibleCreatures
        with BytesIO() as handle:
            with _open_sga(path) as src_sga:
                src_sga.save(handle)
                dst_sga = EssenceFSV2(handle, parse_handle=True, game=game_format)

                for step in src_sga.walk():
                    step: Step

                    assert dst_sga.exists(step.path), step.path
                    with src_sga.opendir(step.path) as src_path:
                        with dst_sga.opendir(step.path) as dst_path:
                            for dir in step.dirs:
                                dir: Info
                                assert dst_path.exists(dir.name), dir.name

                            for file in step.files:
                                file: Info
                                assert dst_path.exists(file.name), file.name
                                with src_path.openbin(file.name) as src_file:
                                    with dst_path.openbin(file.name) as dst_file:
                                        for i, (src_chunk, dst_chunk) in enumerate(
                                            zip(
                                                read_chunks(src_file),
                                                read_chunks(dst_file),
                                            )
                                        ):
                                            assert src_chunk == dst_chunk, (
                                                file.name,
                                                f"Chunk '{i}'",
                                            )

                                src_info = src_path.getinfo(file.name)
                                dst_info = dst_path.getinfo(file.name)
                                assert src_info == dst_info, "Info Different!"


@pytest.mark.skipif(len(_dow_dc_sgas[0]) == 0, reason=f"'{_DOW_DC}' is not installed.")
@pytest.mark.skipif(
    len(_dow_dc_sgas[1]) == 0,
    reason=f"'{_DOW_DC}' has no new content (it was handled by another DOW Test Case).",
)
@pytest.mark.skipif(
    len(_dow_dc_sgas[2]) == 0,
    reason=f"'{_DOW_DC}' has no content less than ({_MAX_SGA_TEST_SIZE} bytes).",
)
@pytest.mark.parametrize("path", _dow_dc_sgas[2])
class TestDawnOfWarDarkCrusade(GameTests): ...


@pytest.mark.skipif(
    len(_dow_gold_sgas[0]) == 0, reason=f"'{_DOW_GOLD}' is not installed."
)
@pytest.mark.skipif(
    len(_dow_gold_sgas[1]) == 0,
    reason=f"'{_DOW_GOLD}' has no new content (it was handled by another DOW Test Case).",
)
@pytest.mark.skipif(
    len(_dow_gold_sgas[2]) == 0,
    reason=f"'{_DOW_GOLD}' has no content less than ({_MAX_SGA_TEST_SIZE} bytes).",
)
@pytest.mark.parametrize("path", _dow_gold_sgas[2])
class TestDawnOfWarGold(GameTests): ...


@pytest.mark.skipif(len(_dow_ss_sgas[0]) == 0, reason=f"'{_DOW_SS}' is not installed.")
@pytest.mark.skipif(
    len(_dow_ss_sgas[1]) == 0,
    reason=f"'{_DOW_SS}' has no new content (it was handled by another DOW Test Case).",
)
@pytest.mark.skipif(
    len(_dow_ss_sgas[2]) == 0,
    reason=f"'{_DOW_SS}' has no content less than ({_MAX_SGA_TEST_SIZE} bytes).",
)
@pytest.mark.parametrize("path", _dow_ss_sgas[2])
class TestDawnOfWarSoulstorm(GameTests): ...


@pytest.mark.skipif(
    len(_imp_creatures_sgas[0]) == 0, reason=f"'{_IMP_CREATURES}' is not installed."
)
@pytest.mark.skipif(
    len(_imp_creatures_sgas[1]) == 0,
    reason=f"'{_IMP_CREATURES}' has no new content (it was handled by another DOW Test Case).",
)
@pytest.mark.skipif(
    len(_imp_creatures_sgas[2]) == 0,
    reason=f"'{_IMP_CREATURES}' has no content less than ({_MAX_SGA_TEST_SIZE} bytes).",
)
@pytest.mark.parametrize("path", _imp_creatures_sgas[2])
class TestImpossibleCreatures(GameTests): ...
