import os
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import List, Dict

import fs
import pytest
from fs.info import Info
from fs.walk import Step
from relic.sga.core.lazyio import read_chunks

from relic.sga.v2.serialization import SgaV2GameFormat
from relic.sga.v2.sgafs import SgaFsV2

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
_installed: Dict[str, List[str]] = {
    game: [str(sga) for sga in (_root / game).rglob("*.sga-backup")] for game in _ALLOWED_GAMES
}

_dow_dc_sgas = _installed[_DOW_DC]
_dow_gold_sgas = _installed[_DOW_DC]
_dow_ss_sgas = _installed[_DOW_DC]
_imp_creatures_sgas = _installed[_IMP_CREATURES]

QUICK = True  # skips slower tests like file MD5 checksums and file CRC checks


@contextmanager
def _open_sga(path: str, **kwargs) -> SgaFsV2:
    game_format: SgaV2GameFormat = None
    if "Dawn of War" in path:
        game_format = SgaV2GameFormat.DawnOfWar
    elif "Impossible Creatures" in path:
        game_format = SgaV2GameFormat.ImpossibleCreatures

    with open(path, "rb") as h:
        yield SgaFsV2(h, parse_handle=True, game=game_format, **kwargs)


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



    def test_repack(self, path:str):
        game_format: SgaV2GameFormat = None
        if "Dawn of War" in path:
            game_format = SgaV2GameFormat.DawnOfWar
        elif "Impossible Creatures" in path:
            game_format = SgaV2GameFormat.ImpossibleCreatures
        with BytesIO() as handle:
            with _open_sga(path) as src_sga:
                src_sga.save(handle)
                dst_sga = SgaFsV2(handle, parse_handle=True, game=game_format)

                for step in src_sga.walk():
                    step:Step

                    assert dst_sga.exists(step.path), step.path
                    with dst_sga.opendir(step.path) as dst_path:
                        for dir in step.dirs:
                            dir:Info
                            assert dst_path.exists(dir.name), dir.name
                        for file in step.files:
                            file: Info
                            assert dst_path.exists(file.name), file.name
                            with src_sga.opendir(step.path) as src_path:
                                with src_path.openbin(file.name) as src_file:
                                    with dst_path.openbin(file.name) as dst_file:
                                        for i, (src_chunk, dst_chunk) in enumerate(zip(read_chunks(src_file),read_chunks(dst_file))):
                                            assert src_chunk == dst_chunk, (file.name, f"Chunk '{i}'")



@pytest.mark.skipif(len(_dow_dc_sgas) == 0, reason=f"'{_DOW_DC}' is not installed.")
@pytest.mark.parametrize("path", _dow_dc_sgas)
class TestDawnOfWarDarkCrusade(GameTests):
    ...


@pytest.mark.skipif(len(_dow_gold_sgas) == 0, reason=f"'{_DOW_GOLD}' is not installed.")
@pytest.mark.parametrize("path", _dow_gold_sgas)
class TestDawnOfWarGold(GameTests):
    ...


@pytest.mark.skipif(len(_dow_ss_sgas) == 0, reason=f"'{_DOW_SS}' is not installed.")
@pytest.mark.parametrize("path", _dow_ss_sgas)
class TestDawnOfWarSoulstorm(GameTests):
    ...


@pytest.mark.skipif(
    len(_imp_creatures_sgas) == 0, reason=f"'{_IMP_CREATURES}' is not installed."
)
@pytest.mark.parametrize("path", _imp_creatures_sgas)
class TestImpossibleCreatures(GameTests):
    ...

