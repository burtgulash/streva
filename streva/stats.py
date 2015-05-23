
import time

class Stats:

    def __init__(self):
        self.operation_stats = {}
        self.queue_size = 0
        self.initialized_at = time.time()

    def register_operation_stats(self, operation_name):
        self.operation_stats[operation_name] = Stats.OperationStats()

    def update_running_stats(self, operation_name, running_time, number_of_runs):
        op_stats = self.operation_stats[operation_name]
        op_stats.runs += number_of_runs
        op_stats.total_time += running_time

    def bottom_line(self):
        s = self.OperationStats()
        s.runs = sum(map(lambda s: s.runs, self.operation_stats.values()))
        s.total_time = sum(map(lambda s: s.total_time, self.operation_stats.values()))
        return s

    def __str__(self):
        grand_total = self.bottom_line()
        calendar_time = time.time() - self.initialized_at
        cpu_time = calendar_time - self.operation_stats["timeouts"].total_time


        strs = []
        for op, stats in self.operation_stats.items():
            s = "Statistics for event {}:\n{}".format(op, str(stats))
            strs.append(s)

        strs.append("Grand total statistics:\n{}".format(str(grand_total)))
        strs.append("Calendar time:    {}".format(time.time() - self.initialized_at))
        strs.append("CPU time:         {}\n".format(cpu_time))

        return "Runtime statistics:\n\n" + "\n".join(strs)


    class OperationStats:

        def __init__(self):
            self.runs = 0
            self.total_time = 0

        def __str__(self):
            avg = self.total_time / self.runs if self.runs else 0
            return ("events processed:         {}\n"
                   "total running time [s]:   {}\n"
                   "average running time [s]: {}\n".format(self.runs, 
                                                           self.total_time, 
                                                           avg))

