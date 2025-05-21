import json
from collections import defaultdict

class WorkflowScheduler:
    def __init__(self):
        self.tasks = []
        self.cloud_servers = []
        self.ect_table = {}  # Execution time table: {task: {vm: time}}
        self.priority_queue = []
        self.task_allocation = defaultdict(list)  # {vm: [task1, task2,...]}
        self.est_values = {}  # Earliest start times
        self.eft_values = {}  # Earliest finish times
        self.entry_task = None
        self.exit_task = None
        self.task_dependencies = {}  # {task: [prerequisites]}
        self.communication_times = {}  # {(task1, task2): time}
        
    def load_input(self, input_file):
        """Load input from JSON file"""
        with open(input_file) as f:
            data = json.load(f)
            
        # Load tasks and identify entry/exit tasks
        self.tasks = data['tasks']
        self.entry_task = data['entry_task']
        self.exit_task = data['exit_task']
        self.task_dependencies = data['dependencies']
        self.communication_times = {tuple(pair.split('-')): time for pair, time in data['communication_times'].items()}
        
        # Load cloud servers and VMs
        self.cloud_servers = data['cloud_servers']
        
        # Load ECT table
        self.ect_table = data['ect_table']
        
    def save_output(self, output_file, makespan, load_balancing, speedup, efficiency, resource_utilization):
        """Save results to a plain text file"""
        with open(output_file, 'w') as f:
            f.write("Task Allocation:\n")
            for vm, tasks in self.task_allocation.items():
                f.write(f"  {vm}: {tasks}\n")
            
            f.write("\nEarliest Start Times (EST):\n")
            for (task, vm), time in self.est_values.items():
                f.write(f"  {task} on {vm}: {time}\n")
            
            f.write("\nEarliest Finish Times (EFT):\n")
            for (task, vm), time in self.eft_values.items():
                f.write(f"  {task} on {vm}: {time}\n")
            
            f.write("\nPerformance Metrics:\n")
            f.write(f"  Makespan: {makespan}\n")
            f.write(f"  Load Balancing: {load_balancing}\n")
            f.write(f"  Speedup: {speedup}\n")
            f.write(f"  Efficiency: {efficiency}%\n")
            f.write(f"  Resource Utilization: {resource_utilization}\n")
        

    def satisfies_precedence_constraints(self, task, allocated_tasks):
        """Check if task's dependencies are satisfied"""
        if task == self.entry_task:
            return True
            
        for dep in self.task_dependencies.get(task, []):
            if dep not in allocated_tasks:
                return False
        return True
    
    def calculate_priority(self, remaining_tasks, allocated_tasks):
        """Calculate priority for each task based on Min-Min heuristic"""
        min_times = {}
        
        for task in remaining_tasks:
            if not self.satisfies_precedence_constraints(task, allocated_tasks):
                continue
                
            min_time = min(self.ect_table[task].values())
            min_times[task] = min_time
            
        if not min_times:
            return None
            
        # Select task with minimum min_time
        selected_task = min(min_times.items(), key=lambda x: x[1])[0]
        return selected_task
    
    def update_ect_values(self, task, vm, time):
        """Update ECT values after task allocation"""
        # In the Min-Min heuristic, we add the execution time to other tasks on the same VM
        for t in self.ect_table:
            if t != task and vm in self.ect_table[t]:
                self.ect_table[t][vm] += time
    
    def calculate_est(self, task, vm):
        """Calculate Earliest Start Time for a task on a VM"""
        if task == self.entry_task:
            return 0
            
        max_time = 0
        for pred in self.task_dependencies.get(task, []):
            # Find when predecessor finished on its VM
            pred_vm = None
            for v, tasks in self.task_allocation.items():
                if pred in tasks:
                    pred_vm = v
                    break
                    
            if pred_vm is None:
                continue
                
            # Communication time is 0 if same cloud server
            comm_time = 0 if vm.split('_')[0] == pred_vm.split('_')[0] else \
                self.communication_times.get((pred, task), 0)
                
            finish_time = self.eft_values.get((pred, pred_vm), 0)
            total_time = finish_time + comm_time
            if total_time > max_time:
                max_time = total_time
                
        return max_time
    
    def calculate_eft(self, task, vm):
        """Calculate Earliest Finish Time for a task on a VM"""
        est = self.calculate_est(task, vm)
        return est + self.ect_table[task][vm]
    
    def allocate_task(self, task):
        """Allocate task to best VM"""
        if task == self.entry_task:
            # Duplicate entry task to all VMs
            for server in self.cloud_servers:
                for vm in server['vms']:
                    self.est_values[(task, vm)] = 0
                    self.eft_values[(task, vm)] = self.ect_table[task][vm]
                    self.task_allocation[vm].append(task)
                    # No need to update ECT for duplicates
            return
            
        # For other tasks, find VM with minimum EFT
        best_vm = None
        min_eft = float('inf')
        best_est = 0
        
        for server in self.cloud_servers:
            for vm in server['vms']:
                est = self.calculate_est(task, vm)
                eft = est + self.ect_table[task][vm]
                if eft < min_eft:
                    min_eft = eft
                    best_est = est
                    best_vm = vm
                    
        if best_vm is None:
            return
            
        # est = self.calculate_est(task, best_vm)
        self.est_values[(task, best_vm)] = best_est
        self.eft_values[(task, best_vm)] = min_eft
        self.task_allocation[best_vm].append(task)
        # self.update_ect_values(task, best_vm, self.ect_table[task][best_vm])
    
    def calculate_makespan(self):
        """Calculate makespan (maximum finish time)"""
        return max(self.eft_values.values(), default=0)
    
    # def calculate_load_balancing(self, makespan):
    #     """Calculate load balancing metric"""
    #     if not makespan:
    #         return 0
            
    #     total_load = 0
    #     for vm, tasks in self.task_allocation.items():
    #         vm_load = sum(self.ect_table[task][vm] for task in tasks)
    #         total_load += vm_load
            
    #     avg_load = total_load / len(self.task_allocation) if self.task_allocation else 0
    #     return avg_load / makespan if makespan else 0
    
    def calculate_load_balancing(self,makespan=None):
    # """Load Balancing = (average load / maximum load) * 100"""
        if not self.task_allocation:
            return 0

        vm_loads = []
        for vm, tasks in self.task_allocation.items():
            vm_load = sum(self.ect_table[task][vm] for task in tasks)
            vm_loads.append(vm_load)

        avg_load = sum(vm_loads) / len(vm_loads)
        max_load = max(vm_loads, default=1)

        return (avg_load / max_load) * 100

    
    def calculate_speedup(self, makespan):
        """Calculate speedup metric"""
        if not makespan:
            return 0
            
        sequential_time = sum(min(self.ect_table[task].values()) for task in self.tasks)
        return sequential_time / makespan if makespan else 0
    
    def calculate_efficiency(self, speedup):
        """Calculate efficiency metric"""
        num_vms = sum(len(server['vms']) for server in self.cloud_servers)
        return (speedup / num_vms) * 100 if num_vms else 0
    
    def calculate_resource_utilization(self, makespan):
    # """Calculate resource utilization percentage based on actual working time"""
        if not makespan:
            return 0

        total_utilization = 0
        num_vms = len(self.task_allocation)

        for vm, tasks in self.task_allocation.items():
            for task in tasks:
                total_utilization += self.ect_table[task][vm]

        utilization = (total_utilization / (makespan * num_vms)) * 100
        return utilization



    # def calculate_resource_utilization(self, makespan):
    #     """Calculate resource utilization metric"""
    #     if not makespan:
    #         return 0
            
    #     total_utilization = 0
    #     num_vms = sum(len(server['vms']) for server in self.cloud_servers)
        
    #     for vm, tasks in self.task_allocation.items():
    #         vm_utilization = sum(self.ect_table[task][vm] for task in tasks)
    #         total_utilization += vm_utilization
            
    #     return total_utilization / (makespan * num_vms) if (makespan and num_vms) else 0
    
    def schedule_workflow(self, input_file, output_file):
        """Main scheduling algorithm"""
        # Load input data
        self.load_input(input_file)
        
        # Phase I: Task selection
        remaining_tasks = set(self.tasks)
        allocated_tasks = set()
        
        while remaining_tasks:
            # Select task with highest priority
            task = self.calculate_priority(remaining_tasks, allocated_tasks)
            if task is None:
                break
                
            self.priority_queue.append(task)
            remaining_tasks.remove(task)
            allocated_tasks.add(task)
            
            # Update ECT values (simplified for this example)
            # In a real implementation, we'd update based on the selected VM
            
        # Phase II: Resource selection and task allocation
        while self.priority_queue:
            task = self.priority_queue.pop(0)
            self.allocate_task(task)
            
        # Calculate QoS parameters
        makespan = self.calculate_makespan()
        load_balancing = self.calculate_load_balancing(makespan)
        speedup = self.calculate_speedup(makespan)
        efficiency = self.calculate_efficiency(speedup)
        resource_utilization = self.calculate_resource_utilization(makespan)
        
        # Save results
        self.save_output(output_file, makespan, load_balancing, speedup, efficiency, resource_utilization)
        
        return {
            'makespan': makespan,
            'load_balancing': load_balancing,
            'speedup': speedup,
            'efficiency': efficiency,
            'resource_utilization': resource_utilization
        }

# Example usage
if __name__ == "__main__":
    
    # Run scheduler
    scheduler = WorkflowScheduler()
    results = scheduler.schedule_workflow('input.json', 'output.txt')
    results2 = scheduler.schedule_workflow('input2.json', 'output2.txt')
    
    print("Scheduling Results:")
    print(f"Makespan: {results['makespan']}")
    print(f"Load Balancing: {results['load_balancing'] * 100:.2f}%")
    print(f"Speedup: {results['speedup']}")
    print(f"Efficiency: {results['efficiency']}%")
    print(f"Resource Utilization: {results['resource_utilization'] * 100:.2f}%")