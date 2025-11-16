import datetime
import json
import logging
import multiprocessing
import sys
import tempfile
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Callable, Any

from relic.sga.core.native.parallel_advanced import (
    UnpackerConfig,
    AdvancedParallelUnpacker,
    ExtractionMethod,
)

logger = logging.getLogger()
loglevel = logging.WARN
logger.setLevel(loglevel)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(loglevel)
logger.addHandler(handler)
VERBOSE = False


@contextmanager
def _timer() -> Generator[Callable[[], float], Any, None]:
    import time as time_module

    t0 = time_module.perf_counter()

    def delta() -> float:
        return time_module.perf_counter() - t0

    yield delta


METHODS = [
    ExtractionMethod.NATIVE,  # parallel read / parallel write
    # ExtractionMethod.Optimized, # batch read-write; update results
    # ExtractionMethod.UltraFast, # batch read-write; delayed update results
    # ExtractionMethod.Serial, # serial read / write # ignored; runtime is linear to size of file, not workers
]
_WORKERS = multiprocessing.cpu_count()
RUN_WORKERS = sorted(
    [
        int(_)
        for _ in {
            *[_WORKERS // (2**p) for p in range(8)],
            _WORKERS,
            _WORKERS * 2,
            _WORKERS * 4,
            _WORKERS * 8,
        }
        if _ > 0
    ]
)


def run_serial(path: str):
    cfg = UnpackerConfig(
        num_workers=multiprocessing.cpu_count(),
        logger=logger,
        disable_gc=True,
        native_files=False,
        verbos=VERBOSE,
    )
    unpacker = AdvancedParallelUnpacker(cfg)

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Extracting <{ExtractionMethod.SERIAL.name}>")
        ts = []
        timings = {ExtractionMethod.SERIAL: ts}
        for run in range(len(RUN_WORKERS)):
            # run is used to get an average; not to test workers
            with _timer() as timer:
                unpacker.extract(path, tmpdir, method=ExtractionMethod.SERIAL)
                time = timer()
                ts.append(timer())
            print(f"Serial [{run}]: {time:.3f}")

        print(f"Serial (Average): {sum(ts)/(len(ts) or 1):.3f}")

    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path("./prof/") / (
            datetime.datetime.now().isoformat().replace(":", "_") + ".json"
        )
        p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("w") as h:
            json.dump(timings, h, indent=4)
    except Exception as e:
        traceback.print_exception(e)


# Looks like workers isn't the bottleneck
def run_worker_metric(path: str, workers: list[int] | None = None):
    workers = workers or RUN_WORKERS
    print(f"Testing with '{', '.join(str(w) for w in workers)}'")

    timings = {}
    RUNS = len(workers)
    for method in METHODS:
        m_timings = timings[method] = []
        for run in range(RUNS):
            run_workers = workers[run]
            cfg = UnpackerConfig(
                num_workers=run_workers,
                logger=logger,
                disable_gc=True,
                native_files=False,
                verbose=VERBOSE,
            )
            unpacker = AdvancedParallelUnpacker(cfg)

            with tempfile.TemporaryDirectory() as outdir:
                print(
                    f"Extracting <{method.name}> - run {run + 1} - workers={run_workers}"
                )
                with _timer() as timer:
                    unpacker.extract(path, outdir, method=method)
                    m_timings.append(timer())
    p = Path("./prof/") / (
        datetime.datetime.now().isoformat().replace(":", "_") + ".json"
    )
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("w") as h:
            json.dump(timings, h, indent=4)
    except Exception as e:
        traceback.print_exception(e)
    return timings


def print_avg_timings(timings: dict[ExtractionMethod, list[float]]):
    for method in METHODS:
        m_timings = timings.get(method,[])
        print(method.name, ":", sum(m_timings) / max(1,len(m_timings)))
        print("\t", ", ".join([str(v) for v in m_timings]))


def print_best_run_workers(timings: dict[ExtractionMethod, list[float]]):
    for run in range(len(RUN_WORKERS)):
        run_timings = {
            m: {index: v for (index, v) in enumerate(timings.get(m, []))}.get(run, 0)
            for m in METHODS
        }
        _m, _t = list(run_timings.items())[0]
        worst = best = _t
        best_method = worst_method = _m
        for m, t in run_timings.items():
            if t > worst:
                worst_method = m
                worst = t
            if t < best:
                best_method = m
                best = t
        variance = abs(best - worst)
        average = sum(run_timings.values()) / len(run_timings)

        print(
            f"Workers: {RUN_WORKERS[run]}\n\tAverage: {average:.3f}\n\tVariance: {variance:.3f}"
        )
        print(
            f"\tBest [{best_method.name}]: {best:.3f}\n\tWorst [{worst_method.name}]: {worst:.3f}"
        )
        print()


def run_comparisons(path: str, runs:int = None):
    cfg = UnpackerConfig(
        num_workers=multiprocessing.cpu_count(),
        logger=logger,
        disable_gc=True,
        native_files=False,
        verbose=VERBOSE,
        precache_dirs=True,
    )
    unpacker = AdvancedParallelUnpacker(cfg)
    _METHODS = [
        ExtractionMethod.SERIAL,
        ExtractionMethod.OPTIMIZED,
        # ExtractionMethod.UltraFast, # Merged into optimized
        ExtractionMethod.NATIVE,
        ExtractionMethod.STREAMING
    ]
    _RUNS = 1  # len(run_workers)
    if runs is None:
        runs = _RUNS
    tot_timings = {}
    for method in _METHODS:
        with tempfile.TemporaryDirectory() as tmpdir:
            tot_ts = []
            tot_timings[method] = tot_ts
            stat_ts = []
            stat_timings = {method: stat_ts}
            for run in range(runs):
                print(f"Extracting <{method.name}> - run {run+1}/{runs}")
                # run is used to get an average; not to test workers
                with _timer() as timer:
                    stats = unpacker.extract(path, tmpdir, method=method)
                    time = timer()
                    tot_ts.append(timer())
                    stat_ts.append(stats.timings)
                print(f"'{method.name}' [{run}]: {time:.3f}")

        print(f"{method.name} (Total Average): {sum(tot_ts)/(len(tot_ts) or 1):.3f}")
        print(
            f"{method.name} (Stat Average): {sum([s.total_time for s in stat_ts])/(len(tot_ts) or 1):.3f}"
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path("./prof/") / (
            datetime.datetime.now().isoformat().replace(":", "_") + ".json"
        )
        p.parent.mkdir(parents=True, exist_ok=True)
    try:
        with p.open("w") as h:
            json.dump(tot_timings, h, indent=4)
    except Exception as e:
        traceback.print_exception(e)
    return tot_timings

def unpack_dir(dir_path: str, num_workers:int = None):
    cfg = UnpackerConfig(
        num_workers=num_workers or multiprocessing.cpu_count(),
        logger=logger,
        disable_gc=True,
        native_files=False,
        verbose=VERBOSE,
        precache_dirs=True,
    )
    archives = list(Path(dir_path).rglob("*.sga"))
    tot_bytes = 0
    total_time = 0
    total_files = 0
    unpacker = AdvancedParallelUnpacker(cfg)

    def print_stats(name:str,value:float,suffix:str, include_per_byte:bool=True):
        print(f"{name}: {value:.2f}{suffix}")
        print(f"\tPer Archive: {value/len(archives):.2f}{suffix}")
        print(f"\tPer File: {value/total_files:.2f}{suffix}")
        if include_per_byte:
            print(f"\tPer Byte: {value/tot_bytes:.2f}{suffix}")

    with tempfile.TemporaryDirectory() as outdir:
        with _timer() as timer:
            for i, archive in enumerate(archives):
                print(f"Extracting <{archive.name}> ({i+1}/{len(archives)})")
                stats = unpacker.extract(str(archive), outdir)
                total_time += stats.timings.total_time
                tot_bytes += stats.extracted_bytes
                total_files += stats.extracted_files

            print_stats("Total Time", timer(), "s")
            print_stats("Stat Time", total_time, "s")
            print_stats("Throughput (B)", tot_bytes / total_time, "Bps", include_per_byte=False)
            print_stats("Throughput (KiB)", tot_bytes / 1024 / total_time, "KiBps", include_per_byte=False)
            print_stats("Throughput (MiB)", tot_bytes / 1024 / 1024 / total_time, "MiBps", include_per_byte=False)
            print_stats("Throughput (GiB)", tot_bytes / 1024 / 1024 / 1024 / total_time, "GiBps", include_per_byte=False)

    # 1 Worker ~ 177MiBps
    # 8 Workers ~ 333MiBps
    # 32 ~ 360MiBps

if __name__ == "__main__":
    _path = sys.argv[1]
    # unpack_dir(_path)
    timings = run_comparisons(_path)
    # with open(_path,"r") as h:
    #     timings = {ExtractionMethod(int(m)):v for m,v in json.load(h).items()}

    print_best_run_workers(timings)
    print_avg_timings(timings)
