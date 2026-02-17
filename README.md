# Connected Components Finder (CCF)

Implementation of the CCF algorithm in Python (PySpark) and Scala (Spark) for finding connected components in large graphs.

Based on: [CCF: Fast and Scalable Connected Component Computation in MapReduce](https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf)

## Project Structure

```text
├── README.md                          # Documentation
├── requirements.txt                   # Python dependencies
├── build.sbt                          # Scala build configuration
├── src/                               # Source code
│   └── main/
│       └── scala/
│           ├── CCF_Main.scala                     # Scala unified runner
│           ├── CCF_RDD.scala                      # Scala RDD implementation
│           ├── CCF_DataFrame.scala                # Scala DataFrame implementation
│           └── CCF_Experimental_Analysis.scala    # Scala performance tests
├── notebooks/                         # Jupyter notebooks
│   ├── Finding_Connected_Components_in_a_Graph_with_PySpark.ipynb
│   └── CCF_PySpark_vs_Scala_Comparison.ipynb
├── results/                           # Output files (CSV, charts)
├── project/                           # SBT build metadata
└── target/                            # Build outputs (JARs)
```

## Quick Start

### Using Python

**1. Install dependencies:**

```bash
# Install all required packages
pip install -r requirements.txt

# Or use a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**2. Open the notebook in Jupyter/VS Code or Google Colab:**

```bash
# From project root
jupyter notebook notebooks/Finding_Connected_Components_in_a_Graph_with_PySpark.ipynb
```

The notebook will automatically install PySpark if needed, then run all cells.

### Using Scala

**1. Compile the project:**

```bash
sbt compile
```

**2. Build JAR with all dependencies:**

```bash
sbt assembly
```

**3. Run with Spark:**

```bash
# Run complete analysis (experimental + sample graphs)
spark-submit --class CCF_Main --master 'local[*]' --driver-memory 4g target/scala-2.13/ccf-spark-scala.jar all

# Run only experimental analysis (RDD vs DataFrame comparison)
spark-submit --class CCF_Main --master 'local[*]' --driver-memory 4g target/scala-2.13/ccf-spark-scala.jar experiment

# Run only RDD implementation on a graph file
spark-submit --class CCF_Main --master 'local[*]' --driver-memory 4g target/scala-2.13/ccf-spark-scala.jar rdd <graph_file>

# Run only DataFrame implementation on a graph file
spark-submit --class CCF_Main --master 'local[*]' --driver-memory 4g target/scala-2.13/ccf-spark-scala.jar dataframe <graph_file>
```

**Available modes:**

- `all` - Run experimental analysis + tests on provided graph files (default)
- `experiment` or `exp` - Run only experimental analysis (RDD vs DataFrame comparison)
- `rdd` - Run only RDD implementation on provided graph file
- `dataframe` or `df` - Run only DataFrame implementation on provided graph file

### Performance Comparison (PySpark vs Scala)

After running both PySpark and Scala implementations, use the comparison notebook to analyze performance differences:

**1. Install Python dependencies (if not already done):**

```bash
pip install -r requirements.txt
```

**2. Generate results from both implementations:**

```bash
# From project root

# Run PySpark notebook first (generates csvs/ccf_experimental_results_pyspark.csv)
jupyter notebook notebooks/Finding_Connected_Components_in_a_Graph_with_PySpark.ipynb

# Then run Scala implementation (generates csvs/ccf_experiment_results_scala.csv):
sbt assembly
spark-submit --class CCF_Main --master 'local[*]' --driver-memory 4g target/scala-2.13/ccf-spark-scala.jar all
```

**3. Open the comparison notebook:**

```bash
jupyter notebook notebooks/CCF_PySpark_vs_Scala_Comparison.ipynb
```

**The comparison notebook provides:**

- Side-by-side performance metrics
- Speedup analysis (Scala vs PySpark)
- Detailed visualizations comparing RDD and DataFrame implementations
- Statistical summaries and recommendations
- Export of combined analysis results to `csvs/` and `graphs/` subdirectories

## Algorithm

The CCF algorithm finds connected components by:

1. Starting with edge pairs (src, dst)
2. Iteratively propagating minimum node IDs to neighbors
3. Stopping when no changes occur
4. Result: each node maps to the smallest ID in its component

## Implementations

Both Python and Scala include:

- RDD implementation (low-level operations)
- DataFrame implementation (SQL-like operations with optimization)
- Experimental analysis comparing performance on graphs of increasing size

## Output

Output files are organized into separate directories:

**CSV Data Files (`csvs/` directory):**

- `csvs/ccf_experimental_results_pyspark.csv` - PySpark performance results
- `csvs/ccf_experiment_results_scala.csv` - Scala performance results
- `csvs/ccf_pyspark_vs_scala_comparison.csv` - Combined comparison data

**Visualization Files (`graphs/` directory):**

- `graphs/ccf_performance_comparison_pyspark.png` - PySpark RDD vs DataFrame charts
- `graphs/ccf_comparison_rdd.png` - RDD implementation comparison (PySpark vs Scala)
- `graphs/ccf_comparison_dataframe.png` - DataFrame implementation comparison (PySpark vs Scala)
- `graphs/ccf_comparison_speedup.png` - Speedup analysis charts
- `graphs/ccf_comparison_all.png` - Combined performance overview

**Console Output:**

- Scala implementation prints detailed tables and metrics to console

## Requirements

### Python

**System Requirements:**

- Python 3.7+
- Java JDK 8 or 11 (required for Spark)

**Python Dependencies:**

- PySpark 3.5.0
- NumPy >= 1.24.0
- Pandas >= 2.0.0
- Matplotlib >= 3.7.0
- Seaborn >= 0.12.0
- Jupyter (optional, for running notebooks)

**Installation:**

All Python dependencies are listed in `requirements.txt`. Install them using:

```bash
# Option 1: Install globally (simple)
pip install -r requirements.txt

# Option 2: Using virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Option 3: For macOS Homebrew Python (externally managed)
python3 -m pip install --break-system-packages --user -r requirements.txt
```

**Note for Windows users:** You may need to install Java JDK and set `JAVA_HOME` environment variable. See the PySpark notebook for detailed Windows setup instructions.

### Scala

- Java 8/11/21 (required for Spark)
- SBT (Scala Build Tool)
- Apache Spark 4.0+ (compatible with Scala 2.13)
- Scala 2.13.12 (automatically managed by SBT)
