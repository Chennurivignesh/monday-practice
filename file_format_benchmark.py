#!/usr/bin/env python3
"""
File Format Benchmark with Hardware Metrics
Benchmarks CSV, XLSX, Parquet, ORC, and Feather formats with:
- File size, read/write times, memory usage, CPU time
- Energy consumption calculations
- Comparison table with percentage savings vs CSV baseline
"""

import pandas as pd
import numpy as np
import time
import tracemalloc
import os
import psutil
from pathlib import Path
import json
from datetime import datetime, timedelta
import random
import string

# Required imports for different formats
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.orc as orc
    import pyarrow.feather as feather
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("Warning: PyArrow not installed. Some formats may not work.")

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    print("Warning: openpyxl not installed. XLSX format will not work.")

try:
    import fastparquet
    FASTPARQUET_AVAILABLE = True
except ImportError:
    FASTPARQUET_AVAILABLE = False
    print("Warning: fastparquet not installed.")

class FileFormatBenchmark:
    def __init__(self, num_rows=500_000):
        self.num_rows = num_rows
        self.df = None
        self.results = {}
        self.cpu_tdp_watts = 65  # Intel i5-13600 TDP in watts
        
    def create_test_dataframe(self):
        """Create a DataFrame with specified number of rows"""
        print(f"Creating DataFrame with {self.num_rows:,} rows...")
        
        # Generate realistic test data
        np.random.seed(42)
        random.seed(42)
        
        data = {
            'id': range(1, self.num_rows + 1),
            'name': [''.join(random.choices(string.ascii_letters, k=10)) for _ in range(self.num_rows)],
            'email': [f"user{i}@example.com" for i in range(self.num_rows)],
            'amount': np.random.uniform(1.0, 10000.0, self.num_rows).round(2),
            'date': pd.date_range('2020-01-01', periods=self.num_rows, freq='1min'),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], self.num_rows)
        }
        
        self.df = pd.DataFrame(data)
        print(f"DataFrame created with {len(self.df):,} rows")
        return self.df
    
    def get_file_size_mb(self, filepath):
        """Get file size in MB"""
        return os.path.getsize(filepath) / (1024 * 1024)
    
    def measure_memory_usage(self, func, *args, **kwargs):
        """Measure peak memory usage of a function"""
        tracemalloc.start()
        start_time = time.time()
        start_cpu = time.process_time()
        
        try:
            result = func(*args, **kwargs)
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            end_time = time.time()
            end_cpu = time.process_time()
            
            return {
                'result': result,
                'peak_memory_mb': peak / (1024 * 1024),
                'execution_time': end_time - start_time,
                'cpu_time': end_cpu - start_cpu
            }
        except Exception as e:
            tracemalloc.stop()
            raise e
    
    def save_csv(self, df, filepath):
        """Save DataFrame to CSV"""
        df.to_csv(filepath, index=False)
        return filepath
    
    def save_xlsx(self, df, filepath):
        """Save DataFrame to XLSX"""
        if not OPENPYXL_AVAILABLE:
            raise ImportError("openpyxl is required for XLSX format")
        df.to_excel(filepath, index=False, engine='openpyxl')
        return filepath
    
    def save_parquet_pyarrow(self, df, filepath):
        """Save DataFrame to Parquet using PyArrow"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for Parquet format")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filepath)
        return filepath
    
    def save_parquet_fastparquet(self, df, filepath):
        """Save DataFrame to Parquet using fastparquet"""
        if not FASTPARQUET_AVAILABLE:
            raise ImportError("fastparquet is required for this format")
        df.to_parquet(filepath, engine='fastparquet', index=False)
        return filepath
    
    def save_orc(self, df, filepath):
        """Save DataFrame to ORC"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for ORC format")
        table = pa.Table.from_pandas(df)
        orc.write_table(table, filepath)
        return filepath
    
    def save_feather(self, df, filepath):
        """Save DataFrame to Feather"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for Feather format")
        df.to_feather(filepath)
        return filepath
    
    def read_csv(self, filepath):
        """Read CSV file"""
        return pd.read_csv(filepath)
    
    def read_xlsx(self, filepath):
        """Read XLSX file"""
        if not OPENPYXL_AVAILABLE:
            raise ImportError("openpyxl is required for XLSX format")
        return pd.read_excel(filepath, engine='openpyxl')
    
    def read_parquet_pyarrow(self, filepath):
        """Read Parquet file using PyArrow"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for Parquet format")
        return pd.read_parquet(filepath, engine='pyarrow')
    
    def read_parquet_fastparquet(self, filepath):
        """Read Parquet file using fastparquet"""
        if not FASTPARQUET_AVAILABLE:
            raise ImportError("fastparquet is required for this format")
        return pd.read_parquet(filepath, engine='fastparquet')
    
    def read_orc(self, filepath):
        """Read ORC file"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for ORC format")
        return pd.read_orc(filepath)
    
    def read_feather(self, filepath):
        """Read Feather file"""
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for Feather format")
        return pd.read_feather(filepath)
    
    def calculate_energy_consumption(self, cpu_time_seconds):
        """Calculate energy consumption in Watt-hours"""
        # Energy (Wh) = Power (W) × Time (h)
        return (self.cpu_tdp_watts * cpu_time_seconds) / 3600
    
    def benchmark_format(self, format_name, save_func, read_func, file_extension):
        """Benchmark a specific file format"""
        print(f"\nBenchmarking {format_name}...")
        
        # File path
        filepath = f"test_data.{file_extension}"
        
        try:
            # Benchmark write operation
            write_stats = self.measure_memory_usage(save_func, self.df, filepath)
            
            # Get file size
            file_size_mb = self.get_file_size_mb(filepath)
            
            # Benchmark read operation
            read_stats = self.measure_memory_usage(read_func, filepath)
            
            # Calculate energy consumption
            write_energy = self.calculate_energy_consumption(write_stats['cpu_time'])
            read_energy = self.calculate_energy_consumption(read_stats['cpu_time'])
            total_energy = write_energy + read_energy
            
            # Store results
            self.results[format_name] = {
                'file_size_mb': file_size_mb,
                'write_time_sec': write_stats['execution_time'],
                'read_time_sec': read_stats['execution_time'],
                'write_memory_mb': write_stats['peak_memory_mb'],
                'read_memory_mb': read_stats['peak_memory_mb'],
                'write_cpu_time': write_stats['cpu_time'],
                'read_cpu_time': read_stats['cpu_time'],
                'total_energy_wh': total_energy,
                'write_energy_wh': write_energy,
                'read_energy_wh': read_energy
            }
            
            print(f"✓ {format_name} completed successfully")
            
            # Clean up
            if os.path.exists(filepath):
                os.remove(filepath)
                
        except Exception as e:
            print(f"✗ {format_name} failed: {str(e)}")
            self.results[format_name] = None
    
    def run_all_benchmarks(self):
        """Run benchmarks for all supported formats"""
        print("Starting file format benchmarks...")
        print(f"Dataset size: {self.num_rows:,} rows")
        print(f"CPU TDP: {self.cpu_tdp_watts}W")
        
        # Create test data
        self.create_test_dataframe()
        
        # Define formats to benchmark
        formats = [
            ('CSV', self.save_csv, self.read_csv, 'csv'),
            ('XLSX', self.save_xlsx, self.read_xlsx, 'xlsx'),
            ('Parquet (PyArrow)', self.save_parquet_pyarrow, self.read_parquet_pyarrow, 'parquet'),
            ('Parquet (FastParquet)', self.save_parquet_fastparquet, self.read_parquet_fastparquet, 'parquet'),
            ('ORC', self.save_orc, self.read_orc, 'orc'),
            ('Feather', self.save_feather, self.read_feather, 'feather')
        ]
        
        # Run benchmarks
        for format_name, save_func, read_func, extension in formats:
            self.benchmark_format(format_name, save_func, read_func, extension)
    
    def calculate_percentage_savings(self):
        """Calculate percentage savings compared to CSV baseline"""
        if 'CSV' not in self.results or self.results['CSV'] is None:
            print("Warning: CSV baseline not available for comparison")
            return
        
        csv_baseline = self.results['CSV']
        
        for format_name, metrics in self.results.items():
            if metrics is None or format_name == 'CSV':
                continue
                
            for metric in ['file_size_mb', 'write_time_sec', 'read_time_sec', 
                          'write_memory_mb', 'read_memory_mb', 'total_energy_wh']:
                baseline_value = csv_baseline[metric]
                current_value = metrics[metric]
                
                if baseline_value > 0:
                    savings_percent = ((baseline_value - current_value) / baseline_value) * 100
                    metrics[f'{metric}_savings_percent'] = savings_percent
    
    def print_comparison_table(self):
        """Print formatted comparison table with all metrics"""
        print("\n" + "="*120)
        print("FILE FORMAT BENCHMARK RESULTS")
        print("="*120)
        
        if not self.results:
            print("No benchmark results available")
            return
        
        # Calculate percentage savings
        self.calculate_percentage_savings()
        
        # Prepare table data
        headers = ['Format', 'Size (MB)', 'Write (s)', 'Read (s)', 'Write Mem (MB)', 
                  'Read Mem (MB)', 'Energy (Wh)', 'Size Savings', 'Time Savings', 'Energy Savings']
        
        rows = []
        for format_name, metrics in self.results.items():
            if metrics is None:
                continue
                
            row = [
                format_name,
                f"{metrics['file_size_mb']:.2f}",
                f"{metrics['write_time_sec']:.3f}",
                f"{metrics['read_time_sec']:.3f}",
                f"{metrics['write_memory_mb']:.2f}",
                f"{metrics['read_memory_mb']:.2f}",
                f"{metrics['total_energy_wh']:.6f}"
            ]
            
            # Add percentage savings (skip for CSV baseline)
            if format_name != 'CSV':
                row.extend([
                    f"{metrics.get('file_size_mb_savings_percent', 0):.1f}%",
                    f"{metrics.get('write_time_sec_savings_percent', 0):.1f}%",
                    f"{metrics.get('total_energy_wh_savings_percent', 0):.1f}%"
                ])
            else:
                row.extend(['Baseline', 'Baseline', 'Baseline'])
            
            rows.append(row)
        
        # Print table
        print(f"{'Format':<20} {'Size':<10} {'Write':<10} {'Read':<10} {'Wr Mem':<12} {'Rd Mem':<12} {'Energy':<12} {'Size %':<10} {'Time %':<10} {'Energy %':<10}")
        print("-" * 120)
        
        for row in rows:
            print(f"{row[0]:<20} {row[1]:<10} {row[2]:<10} {row[3]:<10} {row[4]:<12} {row[5]:<12} {row[6]:<12} {row[7]:<10} {row[8]:<10} {row[9]:<10}")
        
        print("\n" + "="*120)
        print("DETAILED METRICS")
        print("="*120)
        
        # Print detailed metrics for each format
        for format_name, metrics in self.results.items():
            if metrics is None:
                continue
                
            print(f"\n{format_name}:")
            print(f"  File Size: {metrics['file_size_mb']:.2f} MB")
            print(f"  Write Time: {metrics['write_time_sec']:.3f} s (CPU: {metrics['write_cpu_time']:.3f} s)")
            print(f"  Read Time: {metrics['read_time_sec']:.3f} s (CPU: {metrics['read_cpu_time']:.3f} s)")
            print(f"  Write Memory: {metrics['write_memory_mb']:.2f} MB")
            print(f"  Read Memory: {metrics['read_memory_mb']:.2f} MB")
            print(f"  Write Energy: {metrics['write_energy_wh']:.6f} Wh")
            print(f"  Read Energy: {metrics['read_energy_wh']:.6f} Wh")
            print(f"  Total Energy: {metrics['total_energy_wh']:.6f} Wh")
            
            if format_name != 'CSV':
                print(f"  Size Savings vs CSV: {metrics.get('file_size_mb_savings_percent', 0):.1f}%")
                print(f"  Write Time Savings vs CSV: {metrics.get('write_time_sec_savings_percent', 0):.1f}%")
                print(f"  Read Time Savings vs CSV: {metrics.get('read_time_sec_savings_percent', 0):.1f}%")
                print(f"  Energy Savings vs CSV: {metrics.get('total_energy_wh_savings_percent', 0):.1f}%")
    
    def save_results_to_json(self, filename="benchmark_results.json"):
        """Save benchmark results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to {filename}")

def main():
    """Main function to run the benchmark"""
    print("File Format Benchmark with Hardware Metrics")
    print("==========================================")
    
    # Check dependencies
    missing_deps = []
    if not PYARROW_AVAILABLE:
        missing_deps.append("pyarrow")
    if not OPENPYXL_AVAILABLE:
        missing_deps.append("openpyxl")
    if not FASTPARQUET_AVAILABLE:
        missing_deps.append("fastparquet")
    
    if missing_deps:
        print(f"Warning: Missing dependencies: {', '.join(missing_deps)}")
        print("Install with: pip install " + " ".join(missing_deps))
        print()
    
    # Create and run benchmark
    benchmark = FileFormatBenchmark(num_rows=500_000)
    benchmark.run_all_benchmarks()
    benchmark.print_comparison_table()
    benchmark.save_results_to_json()

if __name__ == "__main__":
    main()
