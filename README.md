# global-state-snapshot
Global State Snapshot through a Distributed Banking Example. In this project, we can instantiate 2 branches of a bank and transacting is performed in every 0.5 seconds. Similarly, a branch can initiate a snapshot at any time. Inspector is also present there to control them.

### Platform 
The project was done using Linux filesystem, for the ease of building, use a linux machine.

### Dependencies
- Python 3.8 (minimum)

### Process of running code:
1. Instantiate branches of the bank. Type the command below in two different terminals.
```bash
python main.py -b 
```
2. Instantiate inspector of the bank. 
```bash
python main.py -b 
```
Now the code will run as a simulation. Type 's' in respective banks for snapshot.
