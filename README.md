# TP02 — How to Work with Big Data Files (+5GB)

**Module:** Big Data Analytics  
**Level:** Second-year Master — AI & Data Science  
**Academic year:** 2026/2027

## Objectives
- Demonstrate the memory limitations of loading a very large CSV with standard Pandas.
- Process a CSV incrementally with Pandas chunks of 100,000 rows.
- Use Dask DataFrame for lazy, out-of-core computation.
- Record execution time and peak process memory.

## 1. Setup
Python 3.10+ is recommended.

```bash
python -m venv .venv
```

Windows:
```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Dataset
Place the supplied CSV (larger than 5 GB) in the project directory, or provide its path when prompted.
The scripts do not download a dataset automatically.

## 3. Run
```bash
python tp02_big_data.py
```

Enter the CSV path when prompted, or press Enter to use `big_data.csv` in this folder.

The script runs:
1. Standard Pandas loading test (this can use a lot of RAM and may freeze/crash a low-memory computer).
2. Pandas chunk processing with `chunksize=100_000`.
3. Dask lazy loading and computation.
4. A CSV report named `benchmark_results.csv`.

**Safety:** To avoid destabilizing your computer, the standard Pandas test is disabled by default. Set `RUN_STANDARD_PANDAS=1` in the environment only if you have enough free RAM and are willing to risk an out-of-memory failure.

Windows PowerShell:
```powershell
$env:RUN_STANDARD_PANDAS="1"
python tp02_big_data.py
```

## 4. How it works
- **Standard Pandas:** tries to load the whole file into memory. The required memory may be several times the CSV file size.
- **Pandas chunking:** reads 100,000 rows at a time and updates count, sum, and mean for numeric columns.
- **Dask:** builds a lazy task graph at first; operations are executed when `.compute()` is called. It reads partitions rather than requiring the entire dataset in one Pandas DataFrame.

## 5. Results and discussion
Run the script on the target dataset and retain the generated `benchmark_results.csv`. Actual execution times and RAM values depend on the dataset, hardware, disk, and available memory; do not replace them with invented values.

Expected qualitative comparison:
| Method | Memory behavior | Execution style | Limitation |
|---|---|---|---|
| Standard Pandas | Potentially very high | Eager, single DataFrame | May raise `MemoryError` or cause OS termination |
| Pandas chunks | Bounded by chunk size (plus overhead) | Sequential | Usually requires explicit incremental aggregation |
| Dask DataFrame | Partitioned/out-of-core | Lazy task graph; can parallelize | Scheduler and partition overhead; `.compute()` output must fit memory |

## 6. Upload to GitHub
From this directory:
```bash
git init
git add README.md requirements.txt tp02_big_data.py .gitignore
git commit -m "Complete TP02 big data file processing"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
git push -u origin main
```
Replace the URL with your own GitHub repository URL. Do **not** upload the 5GB dataset to GitHub; keep it locally or use an approved dataset storage service.
