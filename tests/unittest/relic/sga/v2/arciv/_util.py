from typing import List

from relic.sga.v2.arciv import Arciv
from relic.sga.v2.arciv.definitions import TocFolderItem, TocFileItem, TocFolderInfo, TocStorage, TocHeader, TocItem, ArchiveHeader


def assert_arciv_toc_folder_folders_eq(left:List[TocFolderItem], right:List[TocFolderItem]):
    assert len(left) == len(right)
    for l, r in zip(left,right):
        assert_arciv_toc_folder_eq(l, r)


def assert_arciv_toc_file_eq(left:TocFileItem, right:TocFileItem):
    assert left.File == right.File
    assert left.Path == right.Path
    assert left.Size == right.Size
    assert left.Store == right.Store


def assert_arciv_toc_folder_files_eq(left:List[TocFileItem], right:List[TocFileItem]):
    assert len(left) == len(right)
    for l, r in zip(left,right):
        assert_arciv_toc_file_eq(l,r)


def assert_arciv_toc_folder_info_eq(left:TocFolderInfo, right:TocFolderInfo):
    assert left.folder == right.folder
    assert left.path == right.path


def assert_arciv_toc_folder_eq(left:TocFolderItem, right:TocFolderItem):
    assert_arciv_toc_folder_info_eq(left.FolderInfo, right.FolderInfo)
    assert_arciv_toc_folder_files_eq(left.Files, right.Files)
    assert_arciv_toc_folder_folders_eq(left.Folders, right.Folders)


def assert_arciv_toc_storage_eq(left:TocStorage, right:TocStorage):
    assert left.MinSize == right.MinSize
    assert left.MaxSize == right.MaxSize
    assert left.Storage == right.Storage
    assert left.Wildcard == right.Wildcard


def assert_arciv_toc_header_storage_eq(left:List[TocStorage], right:List[TocStorage]):
    assert len(left) == len(right)
    for l, r in zip(left,right):
        assert_arciv_toc_storage_eq(l, r)


def assert_arciv_toc_header_eq(left:TocHeader, right:TocHeader):
    assert left.Alias == right.Alias 
    assert left.Name == right.Name 
    assert left.RootPath == right.RootPath
    assert_arciv_toc_header_storage_eq(left.Storage, right.Storage)


def assert_arciv_toc_item_eq(left:TocItem, right:TocItem):
    assert_arciv_toc_header_eq(left.TOCHeader, right.TOCHeader)
    assert_arciv_toc_folder_eq(left.RootFolder, right.RootFolder)


def assert_arciv_toc_list_eq(left:List[TocItem], right:List[TocItem]):
    assert len(left) == len(right)
    for l, r in zip(left,right):
        assert_arciv_toc_item_eq(l, r)


def assert_arciv_header_eq(left:ArchiveHeader, right:ArchiveHeader):
    assert left.ArchiveName == right.ArchiveName


def assert_arciv_eq(left:Arciv, right:Arciv):
    assert_arciv_header_eq(left.ArchiveHeader, right.ArchiveHeader)
    assert_arciv_toc_list_eq(left.TOCList, right.TOCList)
