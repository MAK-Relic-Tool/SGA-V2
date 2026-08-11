import dataclasses
import logging
import multiprocessing
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Callable, Any

from relic.sga.core.native.handler import SgaReader

from relic.sga.v2.fsspec_.main import SgaV2 as SgaFsSpec
from relic.sga.v2.native.parser import NativeParserV2
from relic.sga.v2.pyfilesystem.definitions import EssenceFSV2 as SgaPyFs

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

@contextmanager
def _timer() -> Generator[Callable[[], float], Any, None]:
    import time as time_module

    t0 = time_module.perf_counter()

    def delta() -> float:
        return time_module.perf_counter() - t0

    yield delta



def compare_open(sga:str) -> None:
    # fsspec is averaging 10x faster than pyfs
    # pyfs is still only .5 seconds (because; like fsspec, we load lazily)
    # saving however; is a chore
    #   pyfs doesn't have a _dirty flag; and ALWAYS saves if able
    print("open")
    with _timer() as t1:
        with SgaFsSpec(sga,autosave=False) as specfs:
            specfs_time = t1()
    print("fsspec: ", specfs_time)
    with _timer() as t2:
        with open(sga,"rb") as f:
            with SgaPyFs(f,parse_handle=True,editable=False) as pyfs:
                pyfs_time = t2()
    print("pyfilesystem: ", pyfs_time)
    _print_speedup(("fsspec",specfs_time),("pyfilesystem",pyfs_time))


def _print_speedup(l:tuple[str,float],r:tuple[str,float]):
    l_valid = isinstance(l[1],float)
    r_valid = isinstance(r[1],float)
    if not l_valid or not r_valid:
        return

    if l[1] < r[1]:
        speedup = r[1] / l[1]
        name = l[0]
    else:
        speedup = l[1] / r[1]
        name = r[0]

    print(name,":","x"+str(round(speedup,1)))

def compare_save_no_change(sga:str) -> None:
    # fsspec is averaging 10x faster than pyfs
    # pyfs is still only .5 seconds (because; like fsspec, we load lazily)
    # saving however; is a chore
    #   pyfs doesn't have a _dirty flag; and ALWAYS saves if able


    def save_fail(*args,**kwargs):
        raise TypeError

    print("save(no changes)")
    with _timer() as t1:
        try:
            with SgaFsSpec(sga) as specfs:
                specfs.save = save_fail
        except TypeError:
            specfs_time = "failed; called save"
        else:
            specfs_time = t1()

    print("fsspec: ", specfs_time)
    with _timer() as t2:
        with open(sga,"r+b") as f:
            try:
                with SgaPyFs(f,parse_handle=True,editable=True) as pyfs:
                    pyfs.save  = save_fail
            except TypeError:
                pyfs_time = "failed; called save"
            else:
                pyfs_time = t2()


    print("pyfilesystem: ", pyfs_time)
    _print_speedup(("fsspec",specfs_time),("pyfilesystem",pyfs_time))

def compare_save_change(sga:str) -> None:
    fsspec_sga = "fsspec.sga"
    pyfs_sga = "pyfilesystem.sga"
    with Path(sga).open("rb") as r:
        b = r.read()
        with open(fsspec_sga, "wb") as w1:
            w1.write(b)
        with open(pyfs_sga, "wb") as w1:
            w1.write(b)

    # fsspec is averaging 10x faster than pyfs
    # pyfs is still only .5 seconds (because; like fsspec, we load lazily)
    # saving however; is a chore
    #   pyfs doesn't have a _dirty flag; and ALWAYS saves if able
    SENTINEL = False
    def fail_if_not_called(f):
        nonlocal SENTINEL
        SENTINEL = False
        def _wrapper(*args,**kwargs):
            nonlocal SENTINEL
            SENTINEL = True
            return f(*args,**kwargs)
        return _wrapper

    def nop(*args,**kwargs):
        pass

    print("save(w/ changes)")
    with _timer() as t1:
        with SgaFsSpec(fsspec_sga) as specfs:
            specfs.touch("/data/touched.md")
            specfs.save = fail_if_not_called(specfs.save)
        specfs_time = t1()
        if not SENTINEL:
            specfs_time = "failed; save was not called"
    print("fsspec: ", specfs_time)
    with _timer() as t2:
        try:
            with open(pyfs_sga,"r+b") as f:
                with SgaPyFs(f,parse_handle=True,editable=True) as pyfs:
                    pyfs.save = fail_if_not_called(pyfs.save)
                    pyfs.openbin("/data/touched.md","w")
                pyfs_time = t2()
                if not SENTINEL:
                    pyfs_time = "failed; save was not called"
        except Exception as e:
            logger.exception(e)
            pyfs_time = "failed; save raised exception"
    print("pyfilesystem: ", pyfs_time)
    _print_speedup(("fsspec",specfs_time),("pyfilesystem",pyfs_time))


    fsspec_readable = False
    pyfs_readable = False
    try:
        with SgaFsSpec(fsspec_sga) as specfs:
            fsspec_readable = True
    except Exception as e:
        logger.exception(e)

    try:
        with open(pyfs_sga, "r+b") as f:
            with SgaPyFs(f, parse_handle=True, editable=False) as pyfs:
                pyfs_readable = True
                pyfs.save = nop
    except Exception as e:
        logger.exception(e)

    print("fsspec readable:",fsspec_readable)
    print("pyfs readable:",pyfs_readable)


def try_read_write_fsspec(sga:str):
    with SgaFsSpec(sga) as specfs:
        specfs.touch("/data/touched.md")
    try:
        with SgaFsSpec(sga) as specfs:
            logger.info("fsspec read successful")
    except Exception as e:
        logger.exception(e)

def try_cmp_fsspec(sga:str):
    def _diff(_e1,_e2):
        def _flatten_nested(_d):
            if not isinstance(_d, dict):
                return _d
            flatd = {}
            for k, v in _d.items():
                _tempf = _flatten_nested(v)
                if isinstance(_tempf, dict):
                    flatd.update(_tempf)
                else:
                    flatd[k] = _tempf
            return flatd


        _e1 = [_flatten_nested(dataclasses.asdict(_)) for _ in _e1]
        _e2 = [_flatten_nested(dataclasses.asdict(_)) for _ in _e2]
        keys = list(_e1[0].keys())
        check_keys = set(keys)
        check_keys.remove("data_offset") # allow data_off to be different
        mismatch_keys = set()
        for __e1, __e2 in zip(_e1, _e2):
            safe_check_keys = set(check_keys)
            for key in safe_check_keys:
                if __e1[key] != __e2[key]:
                    mismatch_keys.add(key)
                    check_keys.remove(key)

        def _build_filtered(_ex):
            return [{k:v for k,v in _.items() if k in mismatch_keys} for _ in _ex]

        return _build_filtered(_e1), _build_filtered(_e2)

    size1 = os.stat(sga).st_size
    with NativeParserV2(sga) as parser:
        entries = parser.parse()
        dataoff = parser._data_block_start
    with SgaReader(sga) as reader:
        blobs = reader.read_files_parallel(entries,multiprocessing.cpu_count())

    with SgaFsSpec(sga) as specfs:
        specfs.save()

    size2 = os.stat(sga).st_size
    with NativeParserV2(sga) as parser:
        entries2 = parser.parse()
        dataoff2 = parser._data_block_start
    with SgaReader(sga) as reader:
        blobs2 = reader.read_files_parallel(entries2, multiprocessing.cpu_count())

    if size1 != size2:
        logger.warning(f"\tSize changed after save! {size1} -> {size2}")

    if len(entries) != len(entries2):
        logger.error(f"\tentries do not match ({len(entries)} != {len(entries2)})")
        return


    exact_failed = False
    diff1, diff2 = _diff(entries, entries2)
    for i, (e1, e2) in enumerate(zip(diff1,diff2)):
        if e1 != e2:
            exact_failed = True
            logger.warning(f"\tEXACT entries do not match!\n\t{e1}\n\t{e2}")
            break

    for i, (b1, b2) in enumerate(zip(blobs,blobs2)):
        if b1.errors or b2.errors:
          exact_failed = True
          logger.warning(f"\tError reading blobs!\n\t{b1.input.data_offset}\t{b1.errors}\n\t{b2.input.data_offset}\t{b2.errors}")

        elif b1.output != b2.output:
            exact_failed = True
            logger.warning(f"\tEXACT blobs do not match!\n\t{b1.output}\n\t{b2.output}")

    if exact_failed:
        logger.error(f"\tentries did not match after saving!")
    else:
        logger.info(f"\tentries match after save")


if __name__ == "__main__":
    _PATH = "./source.sga"
    for sga in Path(sys.argv[1]).glob("**/*.sga"):
        logger.info(f"Processing {sga}")
        with Path(_PATH).open("wb") as _f:
            _f.write(sga.read_bytes())
        try_cmp_fsspec(_PATH)
