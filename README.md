# Modified Min-Min Heuristic Implementation with Load Balancing

## Overview
This repository contains the Python implementation of the **Modified Min-Min Heuristic** for **workflow scheduling in Cloud Computing Environments (CCE)**. The algorithm improves task scheduling by minimizing makespan and balancing virtual machine (VM) loads. It supports deterministic workflows with communication costs and task dependencies.

## Key Features
- **Efficient Task Scheduling:** Uses Modified Min-Min strategy with entry task duplication and communication-aware mapping.
- **Load Balancing:** Balances execution loads across available VMs.
- **Empirical Timing:** Automatically reports scheduling time for benchmarking.
- **Comprehensive Testing:** Validated using multiple synthetic and real-world JSON-based workflows via `pytest`.

## Usage

### 1. Clone the Repository
```bash
git clone https://github.com/shifashah3/Modified-Min-Min-Heuristic.git
cd Modified-Min-Min-Heuristic
```

### 2. Run the Scheduler
```bash
cd src
python modified_min_min.py
```
Make sure the input files like `input.json`, `input2.json`, or any `WF_*.json` are placed in the same directory.

### 3. Run Tests
Tests will check correctness, makespan, speedup, and load balancing across various inputs:
```bash
cd ..
cd test
pytest test_scheduler.py
```

## Input Format
Input should be a `.json` file containing:
- `tasks`: List of task IDs
- `entry_task`, `exit_task`: Task identifiers
- `dependencies`: Predecessor mapping
- `communication_times`: Time between dependent tasks
- `cloud_servers`: List of VM IDs
- `ect_table`: Expected Completion Time for each task-VM pair

## Output
Each output file contains:
- Task-to-VM allocation
- Makespan
- Speedup
- Efficiency
- Load Balancing
- Empirical Execution Time (in seconds)

## Reference
- **[Original Research Paper](https://link.springer.com/article/10.1007/s10586-024-04307-8)**
