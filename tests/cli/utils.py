from contextlib import contextmanager
from os.path import join, abspath
from typing import Optional, Generator

from fs import open_fs
from fs.base import FS
from fs.copy import copy_fs


def get_data_path(*parts:str) -> str:
    data = abspath(join(__file__,"..","..","data"))
    return join(data,*parts)

def get_dataset_path(*parts:str) -> str:
    return get_data_path("dataset",*parts)

@contextmanager
def create_temp_dataset_fs(path:str, identifier:Optional[str]=None) -> Generator[FS,None,None]:
    with open_fs(f"temp://{identifier or ''}") as tmp:
        # Copy files into tmp filesytem
        copy_fs(path,tmp)

        # Fix arciv absolute paths
        for match in tmp.glob("**/*.arciv"):
            match_path:str = match.path
            arciv_txt = tmp.readtext(match_path)
            arciv_txt = arciv_txt.replace("<cwd>",tmp.getsyspath("/"))
            tmp.writetext(match_path,arciv_txt)


        yield tmp

