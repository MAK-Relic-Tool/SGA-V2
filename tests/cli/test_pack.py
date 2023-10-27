from fs.base import FS

from utils import create_temp_dataset_fs, get_dataset_path
import pytest
from relic.core.cli import cli_root


_DATASETS = [
    get_dataset_path("sample-10-26-2023")
]
@pytest.mark.parametrize(
    "dataset",_DATASETS
)
def test_pack(dataset:str) -> None:
    tmp_fs: FS
    with create_temp_dataset_fs(dataset) as tmp_fs:
        for arciv in tmp_fs.glob("**/*.arciv"):
            print(arciv.path)
            sys_path = tmp_fs.getsyspath(arciv.path)
            cli_root.run_with("relic","sga","pack","v2",sys_path)
