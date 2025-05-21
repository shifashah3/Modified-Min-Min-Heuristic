import pytest
import json
import os
import time
from codems import WorkflowScheduler

INPUT_DIR = "input"
OUTPUT_DIR = "output"

@pytest.fixture
def scheduler():
    return WorkflowScheduler()

def load_input_data(file_path):
    with open(file_path) as f:
        return json.load(f)

input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".json")]

@pytest.mark.parametrize("input_filename", input_files)
def test_scheduler_correctness(input_filename, scheduler):

    input_file = os.path.join(INPUT_DIR, input_filename)
    base_name = os.path.splitext(input_filename)[0]
    output_file = os.path.join(OUTPUT_DIR, f"{base_name}_output.txt")

    start_time = time.perf_counter()
    results = scheduler.schedule_workflow(input_file, output_file)
    end_time = time.perf_counter()
    empirical_time = end_time - start_time

    assert results['makespan'] > 0, "Makespan must be positive"
    assert 0 <= results['efficiency'] <= 100, "Efficiency must be between 0 and 100"
    assert results['speedup'] > 0, "Speedup should be greater than 0"

    
    input_data = load_input_data(input_file)

    task_counts = {}
    for vm, tasks in scheduler.task_allocation.items():
        for task in tasks:
            if task != scheduler.entry_task:
                task_counts[task] = task_counts.get(task, 0) + 1

    for task in input_data["tasks"]:
        if task == input_data["entry_task"]:
            continue
        assert task_counts.get(task, 0) == 1, f"Task {task} assigned multiple times!"

    for task in input_data["tasks"]:
        if task == input_data["entry_task"]:
            continue
        deps = input_data["dependencies"].get(task, [])
        for dep in deps:
            task_vm = next((vm for vm, t_list in scheduler.task_allocation.items() if task in t_list), None)
            dep_vm = next((vm for vm, t_list in scheduler.task_allocation.items() if dep in t_list), None)

            task_est = scheduler.est_values.get((task, task_vm), 0)
            dep_eft = scheduler.eft_values.get((dep, dep_vm), 0)

            comm_key = f"{dep}-{task}"
            comm_time = 0 if dep_vm.split('_')[0] == task_vm.split('_')[0] else \
                        input_data["communication_times"].get(comm_key, 0)
            assert task_est >= dep_eft + comm_time, (
                f"Dependency violation: {dep} → {task}, "
                f"EST={task_est}, Dep_EFT={dep_eft}, Comm={comm_time}"
            )

    # Check EFT = EST + ECT
    for (task, vm), est in scheduler.est_values.items():
        eft = scheduler.eft_values.get((task, vm))
        ect = input_data["ect_table"][task][vm]
        assert eft == est + ect, f"EFT inconsistency: {task} on {vm}, EST={est}, ECT={ect}, EFT={eft}"

    with open(output_file, 'a') as f:
        f.write(f"\nEmpirical Time: {empirical_time:.4f} seconds\n")

    print(f"Empirical Time for {input_filename}: {empirical_time:.4f} seconds")
