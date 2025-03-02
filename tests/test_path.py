# from typing import Optional, List
#
# import pytest
# from relic.sga.v2.essencefs.definitions import SgaPathResolver
# from relic.sga.v2.essencefs.pathing import PureSgaPath
#
# _parts = (
#     ["test"],
#     ["fold", "file"],
#     ["Upper"],
#     ["Upper", "Downer"],
#     ["Up", "down", "left", "right", "B", "A", "START"],
# )
# _parts_ids = ["/".join(_) for _ in _parts]
# _aliases = "drive", None
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# @pytest.mark.parametrize("alias", _aliases)
# def test_build_str(parts: List[str], alias: Optional[str]):
#     pather = SgaPathResolver.build(*parts, alias=alias)
#     purePath = (
#         PureSgaPath(f"{alias}:", *parts) if alias is not None else PureSgaPath(*parts)
#     )
#     purePathStr = str(purePath)
#
#     assert purePathStr == pather
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# @pytest.mark.parametrize("alias", _aliases)
# def test_parse(parts: List[str], alias: Optional[str]):
#     path = SgaPathResolver.build(*parts, alias=alias)
#     alias, stem = SgaPathResolver.parse(path)
#     purePath = (
#         PureSgaPath(f"{alias}:", *parts) if alias is not None else PureSgaPath(*parts)
#     )
#
#     assert purePath.drive == (f"{alias}:" if alias is not None else "")
#     assert (
#         purePath.root
#         + PureSgaPath._flavour.sep.join(purePath.parts[(0 if alias is None else 1) :])
#         == stem
#     )
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_split_parts(parts: List[str]):
#     alias = None
#     path = SgaPathResolver.build(*parts, alias=alias)
#     purePath = PureSgaPath.Aliased(*parts, alias=alias)
#     parts = SgaPathResolver.split_parts(path, False)
#
#     assert purePath.parts == tuple(parts)
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_join(parts: List[str]):
#     path = SgaPathResolver.join(*parts)
#     purePath = PureSgaPath.Aliased(*parts)
#
#     return str(purePath) == path
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_split(parts: List[str]):
#     path = SgaPathResolver.build(*parts)
#     purePath = PureSgaPath.Aliased(*parts)
#     head, tail = SgaPathResolver.split(path)
#     pHead, pTail = (
#         PureSgaPath._flavour.sep.join(purePath.parts[:-1]),
#         purePath.parts[-1],
#     )
#
#     assert pHead == head
#     assert pTail == tail
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_strip_root(parts: List[str]):
#     path = SgaPathResolver.build(*parts)
#     stripped = SgaPathResolver.strip_root(path)
#     purePath = PureSgaPath.Aliased(*parts)
#     assert purePath.tail == stripped
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_dirname(parts: List[str]):
#     path = SgaPathResolver.build(*parts)
#     dir = SgaPathResolver.dirname(path)
#     purePath = PureSgaPath.Aliased(*parts)
#     parent = str(purePath.parent)
#     if parent == ".":
#         parent = ""
#     assert parent == dir
#
#
# @pytest.mark.parametrize("parts", _parts, ids=_parts_ids)
# def test_basename(parts: List[str]):
#     path = SgaPathResolver.build(*parts)
#     name = SgaPathResolver.basename(path)
#     purePath = PureSgaPath.Aliased(*parts)
#     assert str(purePath.name) == name
