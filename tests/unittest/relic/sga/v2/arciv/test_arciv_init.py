from io import StringIO
from pathlib import Path
from typing import Dict, Union, Optional, List

import pytest
from relic.sga.core import StorageType

from relic.sga.v2.arciv import (
    Arciv,
    dumps,
    load,
    ArcivEncoder,
    parses,
    parse,
    dump,
    loads,
)
from relic.sga.v2.arciv.definitions import (
    ArchiveHeader,
    TocItem,
    TocFolderItem,
    TocHeader,
    TocStorage,
    TocFolderInfo,
    TocFileItem,
)

pytest.register_assert_rewrite("tests.unittest.relic.sga.v2.arciv._util")
from tests.unittest.relic.sga.v2.arciv._util import assert_arciv_eq

SAMPLE_ARCIV_STR = r"""
Archive =
{
	ArchiveHeader =
	{
		ArchiveName = "sample"
	},
	TOCList =
	{
		{
			TOCHeader =
			{
				Alias = "data",
				Name = "testdata",
				RootPath = [[\Root]],
				Storage =
				{
					{
						MinSize = -1,
						MaxSize = -1,
						Storage = 0,
						Wildcard = "*.*"
					}
				}
			},
			RootFolder =
			{
				Files =
				{
					{
						File = "store.txt",
						Path = [[\Root\Samples\store.txt]],
						Size = 0,
						Store = 0
					}
				},
				Folders =
				{
				},
				FolderInfo =
				{
					folder = "Samples",
					path = [[\Root\Samples]]
				}
			}
		}
	}
}
""".lstrip()  # Ignore leading/trailing whitespace

SAMPLE_ARCIV = Arciv(
    ArchiveHeader=ArchiveHeader("sample"),
    TOCList=[
        TocItem(
            RootFolder=TocFolderItem(
                FolderInfo=TocFolderInfo(folder="Samples", path=Path(r"\Root\Samples")),
                Files=[
                    TocFileItem(
                        File="store.txt",
                        Path=Path(r"\Root\Samples\store.txt"),
                        Size=0,
                        Store=StorageType.STORE,
                    )
                ],
                Folders=[],
            ),
            TOCHeader=TocHeader(
                Alias="data",
                Name="testdata",
                RootPath=Path("\\Root"),
                Storage=[
                    TocStorage(
                        MaxSize=-1,
                        MinSize=-1,
                        Storage=StorageType.STORE,
                        Wildcard="*.*",
                    )
                ],
            ),
        )
    ],
)

_ARCIV_LOAD_TESTS = [(SAMPLE_ARCIV_STR, SAMPLE_ARCIV)]
_ARCIV_LOAD_TEST_IDS = list(range(len(_ARCIV_LOAD_TESTS)))


# Ids required because test cache will crash with arciv string
@pytest.mark.parametrize(
    ["arciv_input", "expected"], _ARCIV_LOAD_TESTS, ids=_ARCIV_LOAD_TEST_IDS
)
class TestLoadArciv:
    def test_load(self, arciv_input: str, expected: Arciv):
        with StringIO(arciv_input) as h:
            result = load(h)
            assert_arciv_eq(result, expected)

    def test_loads(self, arciv_input: str, expected: Arciv):
        result = loads(arciv_input)
        assert_arciv_eq(result, expected)


def _get_dict_from_arciv(a: Arciv):
    encoder = ArcivEncoder()

    def handle(_o: object):
        o = encoder.default(_o)
        if isinstance(o, dict):
            return {k: handle(v) for k, v in o.items()}
        if isinstance(o, list):
            return [handle(v) for i, v in enumerate(o)]
        return o

    return handle(a)


_ARCIV_PARSE_TESTS = [(SAMPLE_ARCIV_STR, _get_dict_from_arciv(SAMPLE_ARCIV))]
_ARCIV_PARSE_TEST_IDS = list(range(len(_ARCIV_PARSE_TESTS)))


@pytest.mark.parametrize(
    ["arciv_input", "expected"], _ARCIV_PARSE_TESTS, ids=_ARCIV_PARSE_TEST_IDS
)
class TestParseArciv:
    @staticmethod
    def compare(result, expect_tree):
        result_tree = _get_dict_from_arciv(result)

        def _extend_key(key: List[str], new: str):
            return [*key, new]

        def cmp(l, r, key: List[str]):
            if isinstance(l, dict):
                assert isinstance(r, dict), key
                assert set(l.keys()) == set(r.keys()), key
                for k in l.keys():
                    cmp(l[k], r[k], _extend_key(key, k)), key
            elif isinstance(l, list):
                assert isinstance(r, list), key
                assert len(l) == len(r), key
                for i in range(len(l)):
                    cmp(l[i], r[i], _extend_key(key, str(i)))
            else:
                assert l == r, key

        cmp(result_tree, expect_tree, [])

    def test_load(self, arciv_input: str, expected: Dict[str, object]):
        with StringIO(arciv_input) as h:
            result = parse(h)
            self.compare(result, expected)

    def test_loads(self, arciv_input: str, expected: Dict[str, object]):
        result = parses(arciv_input)
        self.compare(result, expected)


_ARCIV_DUMP_TESTS = _ARCIV_PARSE_TESTS
_ARCIV_DUMP_TEST_IDS = list(range(len(_ARCIV_DUMP_TESTS)))


@pytest.mark.parametrize(
    ["expected", "arciv_input"], _ARCIV_DUMP_TESTS, ids=_ARCIV_DUMP_TEST_IDS
)
class TestDumpArciv:
    def test_dump(self, arciv_input: Union[Arciv, Dict[str, object]], expected: str):
        with StringIO() as h:
            dump(h, arciv_input)
            result = h.getvalue()
        assert result == expected

    def test_dumps(self, arciv_input: Union[Arciv, Dict[str, object]], expected: str):
        result = dumps(arciv_input)
        assert result == expected
