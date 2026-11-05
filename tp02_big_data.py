
from __future__ import annotations

import os
import time
import threading
from pathlib import Path

import pandas as pd
import psutil


CHUNK_SIZE = 100_000
DEFAULT_FILE = "big_data.csv"
RESULTS_FILE = "benchmark_results.csv"


class PeakMemoryMonitor:
    """Samples this Python process's RSS and records the peak during a task."""

    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.peak_mb = 0.0
        self._stop = threading.Event()
        self._thread = None

    def _sample(self):
        while not self._stop.is_set():
            try:
                current_mb = self.process.memory_info().rss / (1024 ** 2)
                self.peak_mb = max(self.peak_mb, current_mb)
            except (psutil.Error, OSError):
                pass
            self._stop.wait(self.interval)

    def __enter__(self):
        try:
            self.peak_mb = self.process.memory_info().rss / (1024 ** 2)
        except (psutil.Error, OSError):
            self.peak_mb = 0.0
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)


def record_result(rows, method, status, elapsed, peak_mb, details=""):
    rows.append({
        "method": method,
        "status": status,
        "execution_time_seconds": round(elapsed, 4),
        "peak_process_ram_mb": round(peak_mb, 2),
        "details": details,
    })


def standard_pandas_test(file_path: Path, results: list[dict]):
    """Task 1: attempt to read the whole CSV in one DataFrame."""
    print("\n=== Task 1: Standard Pandas (OOM challenge) ===")
    print("Warning: this may consume substantial RAM and destabilize a low-memory system.")
    start = time.perf_counter()
    status, details = "success", ""
    with PeakMemoryMonitor() as monitor:
        try:
            df = pd.read_csv(file_path)
            rows = len(df)
            columns = len(df.columns)
            memory_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
            details = (
                f"Loaded {rows:,} rows and {columns} columns; "
                f"DataFrame memory={memory_mb:.2f} MB"
            )
            print(details)
            del df
        except MemoryError as exc:
            status = "MemoryError"
            details = repr(exc)
            print("MemoryError: the complete dataset could not fit in available memory.")
        except Exception as exc:
            status = "error"
            details = f"{type(exc).__name__}: {exc}"
            print(f"Read failed: {details}")
    elapsed = time.perf_counter() - start
    record_result(results, "Standard Pandas", status, elapsed, monitor.peak_mb, details)


def pandas_chunking(file_path: Path, results: list[dict]):
    """Task 2: process 100,000-row chunks and compute numeric summaries."""
    print("\n=== Task 2: Pandas chunking ===")
    start = time.perf_counter()
    status, details = "success", ""
    total_rows = 0
    numeric_count = {}
    numeric_sum = {}
    numeric_min = {}
    numeric_max = {}

    with PeakMemoryMonitor() as monitor:
        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                total_rows += len(chunk)
                numeric = chunk.select_dtypes(include="number")
                for col in numeric.columns:
                    series = numeric[col]
                    count = int(series.count())
                    if col not in numeric_count:
                        numeric_count[col] = 0
                        numeric_sum[col] = 0.0
                        numeric_min[col] = None
                        numeric_max[col] = None

                    numeric_count[col] += count
                    numeric_sum[col] += float(series.sum(skipna=True))
                    if count:
                        cmin = float(series.min(skipna=True))
                        cmax = float(series.max(skipna=True))
                        numeric_min[col] = cmin if numeric_min[col] is None else min(numeric_min[col], cmin)
                        numeric_max[col] = cmax if numeric_max[col] is None else max(numeric_max[col], cmax)

            summaries = []
            for col in numeric_count:
                count = numeric_count[col]
                mean = numeric_sum[col] / count if count else float("nan")
                summaries.append(
                    f"{col}: count={count:,}, sum={numeric_sum[col]:.6g}, "
                    f"mean={mean:.6g}, min={numeric_min[col]}, max={numeric_max[col]}"
                )
            details = f"Rows processed={total_rows:,}; " + (" | ".join(summaries) if summaries else "No numeric columns detected")
            print(f"Rows processed: {total_rows:,}")
            print("Incremental numeric summaries:")
            for item in summaries:
                print("  " + item)
        except Exception as exc:
            status = "error"
            details = f"{type(exc).__name__}: {exc}"
            print(f"Chunking failed: {details}")

    elapsed = time.perf_counter() - start
    record_result(results, f"Pandas chunks ({CHUNK_SIZE:,})", status, elapsed, monitor.peak_mb, details)


def dask_test(file_path: Path, results: list[dict]):
    """Task 3: build a lazy Dask graph and compute numeric aggregates."""
    print("\n=== Task 3: Dask DataFrame ===")
    try:
        import dask
        import dask.dataframe as dd
    except ImportError:
        print("Dask is not installed. Run: pip install 'dask[dataframe]'")
        record_result(results, "Dask DataFrame", "not installed", 0, 0, "Install dask[dataframe]")
        return

    start = time.perf_counter()
    status, details = "success", ""
    with PeakMemoryMonitor() as monitor:
        try:
        
            ddf = dd.read_csv(str(file_path), blocksize="64MB")
            print(f"Lazy DataFrame created: {ddf.npartitions} partitions")
            print("Columns/dtypes:", ddf.dtypes.to_dict())
            print("Task graph is created; computation starts when compute() is called.")

            numeric_cols = list(ddf.select_dtypes(include="number").columns)
            if numeric_cols:
               
                numeric = ddf[numeric_cols]
                count_expr = numeric.count()
                sum_expr = numeric.sum()
                mean_expr = numeric.mean()
                min_expr = numeric.min()
                max_expr = numeric.max()
                counts, sums, means, mins, maxs = dask.compute(
                    count_expr, sum_expr, mean_expr, min_expr, max_expr
                )
                computed = pd.DataFrame({
                    "count": counts,
                    "sum": sums,
                    "mean": means,
                    "min": mins,
                    "max": maxs,
                })
                details = f"Partitions={ddf.npartitions}; numeric columns={numeric_cols}; result:\n{computed.to_string()}"
                print("Computed numeric aggregates:")
                print(computed)
            else:
                # For CSVs whose inferred types are strings, count rows as a
                # basic execution check; users can specify dtype in read_csv.
                count_rows = ddf.map_partitions(len).sum().compute()
                details = f"Partitions={ddf.npartitions}; rows={int(count_rows)}; no numeric columns inferred"
                print(details)
        except Exception as exc:
            status = "error"
            details = f"{type(exc).__name__}: {exc}"
            print(f"Dask failed: {details}")

    elapsed = time.perf_counter() - start
    record_result(results, "Dask DataFrame", status, elapsed, monitor.peak_mb, details)


def main():
    supplied = input(f"CSV path [{DEFAULT_FILE}]: ").strip()
    file_path = Path(supplied or DEFAULT_FILE).expanduser()

    if not file_path.is_file():
        print(f"\nERROR: File not found: {file_path.resolve()}")
        print("Place your large CSV in this folder as big_data.csv, or enter its full path.")
        return

    print(f"\nDataset: {file_path.resolve()}")
    print(f"File size: {file_path.stat().st_size / (1024 ** 3):.3f} GiB")
    print(f"Chunk size: {CHUNK_SIZE:,} rows")
    print(f"Available system RAM: {psutil.virtual_memory().available / (1024 ** 3):.2f} GiB")

    results = []
    # Safety-first: full loading is disabled unless explicitly enabled.
    if os.getenv("RUN_STANDARD_PANDAS", "0") == "1":
        standard_pandas_test(file_path, results)
    else:
        print("\n=== Task 1: Standard Pandas (skipped for safety) ===")
        print("To attempt full loading, set RUN_STANDARD_PANDAS=1 after assessing available RAM.")
        record_result(
            results, "Standard Pandas", "skipped for safety", 0, 0,
            "Full-file loading is opt-in; it may cause MemoryError or OS termination."
        )

    pandas_chunking(file_path, results)
    dask_test(file_path, results)

    pd.DataFrame(results).to_csv(RESULTS_FILE, index=False)
    print(f"\nBenchmark report saved to: {Path(RESULTS_FILE).resolve()}")
    print("\n=== Benchmark summary ===")
    print(pd.DataFrame(results)[["method", "status", "execution_time_seconds", "peak_process_ram_mb"]].to_string(index=False))
    print("\nNote: measurements are from this machine and this run; do not present them as universal benchmarks.")


if __name__ == "__main__":
    main()
