# File Format Benchmark

A comprehensive Python script to benchmark different file formats (CSV, XLSX, Parquet, ORC, Feather) with hardware metrics including file size, read/write times, memory usage, CPU time, and energy consumption.

## Features

- **Multiple Formats**: CSV, XLSX, Parquet (PyArrow & FastParquet), ORC, Feather
- **Hardware Metrics**: File size, read/write times, memory usage, CPU time
- **Energy Consumption**: Calculates energy usage based on CPU time and TDP
- **Comparison Tables**: Shows percentage savings vs CSV baseline
- **Detailed Results**: Saves results to JSON file

## Requirements

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Or install individually:
```bash
pip install pandas numpy pyarrow openpyxl fastparquet psutil
```

## Usage

Run the benchmark with the default 500,000 rows:

```bash
python file_format_benchmark.py
```

## Output

The script provides:
1. **Console Output**: Formatted comparison table with all metrics
2. **JSON File**: `benchmark_results.json` with detailed results
3. **Percentage Savings**: Comparison vs CSV baseline

## Metrics Measured

- **File Size**: Storage space in MB
- **Write Time**: Time to save file in seconds
- **Read Time**: Time to load file in seconds
- **Memory Usage**: Peak memory during operations (MB)
- **CPU Time**: Actual CPU processing time
- **Energy Consumption**: Estimated energy usage in Watt-hours

## Example Results

```
Format               Size       Write      Read       Wr Mem       Rd Mem       Energy       Size %     Time %     Energy %  
------------------------------------------------------------------------------------------------------------------------
CSV                  33.54      11.918     1.009      4.24         159.20       0.231724     Baseline   Baseline   Baseline  
Parquet (PyArrow)    17.22      0.160      0.514      0.10         66.21        0.011089     48.6%      98.7%      95.2%     
ORC                  19.49      0.096      0.392      0.02         65.71        0.008788     41.9%      99.2%      96.2%     
Feather              21.64      0.048      0.384      0.03         65.71        0.008465     35.5%      99.6%      96.3%     
```

## Configuration

You can modify the benchmark by changing:
- `num_rows`: Number of rows in test dataset (default: 500,000)
- `cpu_tdp_watts`: CPU TDP in watts (default: 65W for i5-13600)

## Notes

- Results may vary based on hardware specifications
- Energy consumption is estimated based on CPU TDP
- All temporary files are cleaned up after benchmarking
