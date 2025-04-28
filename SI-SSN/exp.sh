#!/bin/bash

# Script to run SI experiments with varying threads and read ratios
# Experiment 1: Vary threads from 2 to 32, keep read ratio constant
# Experiment 2: Keep threads constant at 8, vary read ratio from 0.1 to 0.9

# Compilation (adjust compiler flags as needed)
echo "Compiling the program..."
g++ -std=c++17 -pthread -o si_experiment main.cpp

# Constants for experiments
M=1000           # Number of data items
NUM_TRANS=100    # Transactions per thread
CONST_VAL=100    # Maximum value for writes
NUM_ITERS=10     # Operations per transaction
LAMBDA=10        # Mean delay between operations (ms)
DEFAULT_READ_RATIO=0.7  # Default read ratio

# Function to run a single experiment
run_experiment() {
    local threads=$1
    local read_ratio=$2
    local output_dir=$3
    
    echo "Running experiment with threads=$threads, read_ratio=$read_ratio"
    
    # Create experiment directory
    mkdir -p "$output_dir"
    
    # Create input parameter file
    echo "$threads $M $NUM_TRANS $CONST_VAL $NUM_ITERS $LAMBDA $read_ratio" > inp-params.txt
    
    # Run the experiment
    ./a.out
    
    # Save results to the experiment directory
    cp si_result.txt "$output_dir/result_t${threads}_r${read_ratio}.txt"
    cp si_log.txt "$output_dir/log_t${threads}_r${read_ratio}.txt"
    
    # Extract key metrics for summary
    commits_per_sec=$(grep "Commits per second" si_result.txt | awk '{print $4}')
    aborts_per_sec=$(grep "Aborts per second" si_result.txt | awk '{print $4}')
    
    echo "$threads,$read_ratio,$commits_per_sec,$aborts_per_sec" >> "$output_dir/summary.csv"
}

# Experiment 1: Varying threads
echo "Starting Experiment 1: Varying thread counts from 2 to 32"
exp1_dir="experiment_vary_threads"
mkdir -p "$exp1_dir"
echo "threads,read_ratio,commits_per_sec,aborts_per_sec" > "$exp1_dir/summary.csv"

for threads in 2 4 8 16 24 32; do
    run_experiment $threads $DEFAULT_READ_RATIO "$exp1_dir"
done

# Experiment 2: Varying read ratio
echo "Starting Experiment 2: Varying read ratios from 0.1 to 0.9"
exp2_dir="experiment_vary_readratio"
mkdir -p "$exp2_dir"
echo "threads,read_ratio,commits_per_sec,aborts_per_sec" > "$exp2_dir/summary.csv"

for read_ratio in 0.1 0.3 0.5 0.7 0.9; do
    run_experiment 8 $read_ratio "$exp2_dir"
done

# Generate plots (if gnuplot is available)
if command -v gnuplot >/dev/null 2>&1; then
    echo "Generating plots with gnuplot"
    
    # Plot for varying threads
    cat > plot_threads.gp << EOF
set terminal png size 800,600
set output "experiment_vary_threads/throughput_vs_threads.png"
set title "Transaction Throughput vs. Number of Threads"
set xlabel "Number of Threads"
set ylabel "Transactions per Second"
set key outside
set grid
plot "experiment_vary_threads/summary.csv" using 1:3 with linespoints title "Commits/sec", \
     "experiment_vary_threads/summary.csv" using 1:4 with linespoints title "Aborts/sec"
EOF
    gnuplot plot_threads.gp
    
    # Plot for varying read ratios
    cat > plot_readratio.gp << EOF
set terminal png size 800,600
set output "experiment_vary_readratio/throughput_vs_readratio.png"
set title "Transaction Throughput vs. Read Ratio"
set xlabel "Read Ratio"
set ylabel "Transactions per Second"
set key outside
set grid
plot "experiment_vary_readratio/summary.csv" using 2:3 with linespoints title "Commits/sec", \
     "experiment_vary_readratio/summary.csv" using 2:4 with linespoints title "Aborts/sec"
EOF
    gnuplot plot_readratio.gp
    
    rm plot_threads.gp plot_readratio.gp
else
    echo "gnuplot not found - skipping plot generation"
fi

echo "Experiments completed!"
echo "Results for thread variation are in: $exp1_dir"
echo "Results for read ratio variation are in: $exp2_dir"