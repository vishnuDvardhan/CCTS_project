import matplotlib.pyplot as plt
import pandas as pd
import os

# Create directories for output
os.makedirs("plots", exist_ok=True)

# --- Thread Variation Comparison ---
# Load the two experiment summaries
thread_data1 = pd.read_csv("SI/experiment_vary_threads/summary.csv")
thread_data2 = pd.read_csv("SI-SSN/experiment_vary_threads/summary.csv")

# Create the comparison plot
plt.figure(figsize=(12, 8))

# Plot commits per second
plt.subplot(2, 1, 1)
plt.plot(thread_data1['threads'], thread_data1['commits_per_sec'], 'o-', label='SI - Commits/sec')
plt.plot(thread_data2['threads'], thread_data2['commits_per_sec'], 's-', label='SI+SSN - Commits/sec')
plt.xlabel('Number of Threads')
plt.ylabel('Commits per Second')
plt.title('Commit Throughput Comparison')
plt.grid(True)
plt.legend()

# Plot aborts per second
plt.subplot(2, 1, 2)
plt.plot(thread_data1['threads'], thread_data1['aborts_per_sec'], 'o-', label='SI - Aborts/sec')
plt.plot(thread_data2['threads'], thread_data2['aborts_per_sec'], 's-', label='SI+SSN - Aborts/sec')
plt.xlabel('Number of Threads')
plt.ylabel('Aborts per Second')
plt.title('Abort Rate Comparison')
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.savefig("thread_comparison.png")

# --- Read Ratio Variation Comparison ---
# Load the two experiment summaries
ratio_data1 = pd.read_csv("SI/experiment_vary_readratio/summary.csv")
ratio_data2 = pd.read_csv("SI-SSN/experiment_vary_readratio/summary.csv")

# Create the comparison plot
plt.figure(figsize=(12, 8))

# Plot commits per second
plt.subplot(2, 1, 1)
plt.plot(ratio_data1['read_ratio'], ratio_data1['commits_per_sec'], 'o-', label='SI - Commits/sec')
plt.plot(ratio_data2['read_ratio'], ratio_data2['commits_per_sec'], 's-', label='SI+SSN - Commits/sec')
plt.xlabel('Read Ratio')
plt.ylabel('Commits per Second')
plt.title('Commit Throughput Comparison')
plt.grid(True)
plt.legend()

# Plot aborts per second
plt.subplot(2, 1, 2)
plt.plot(ratio_data1['read_ratio'], ratio_data1['aborts_per_sec'], 'o-', label='SI - Aborts/sec')
plt.plot(ratio_data2['read_ratio'], ratio_data2['aborts_per_sec'], 's-', label='SI+SSN - Aborts/sec')
plt.xlabel('Read Ratio')
plt.ylabel('Aborts per Second')
plt.title('Abort Rate Comparison')
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.savefig("read_ratio_comparison.png")

print("Comparison plots have been saved to the 'plots' directory")