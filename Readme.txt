Elasticsearch
Didn't have much time to work on this part so at the moment only 1 bonus
is done (exams, sorry)

Redis
Python version used: 2.7.5
master.py starts one process from worker.py every M seconds.
The M variable is in the master.py file while all the others are in worker.py.
The workers have 3 threads each: one for working, one for updating the
heartbeat and one for killing the whole process.