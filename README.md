# Chandy and Lamport's Global Snapshot Algorithm

It is the implementation of [Chandy and Lamport's snapshot algorithm](https://lamport.azurewebsites.net/pubs/chandy.pdf) with distributed banking as an example.   
In this project, we instantiate *three* bank branches before the inspector records the global state, by default.   
We can change it by adding new branch information in the `config.yml` file under the section `branches`.   
By default, transactions are performed every *0.5 seconds*. We can change it too by modifying the `time_step` variable in `config.yml` under the section `bank`.   
A few other options are also available in the same config_file.

**Note:** A branch can initiate a snapshot at any time by pressing **`s`** in its terminal.

## Platform
Although thorough testing of the project was done on Ubuntu and Arch Linux, it should run fine on other operating systems as well.

## Dependencies

The project uses the libraries provided on the default installation of Python.
The only dependency is `pyyaml` which is required for reading configuration file.   
You can install it by running the following command:

```sh
# if Python Version 2 and 3 are both installed on your system,
# you might need to use `pip3` instead of pip.
pip install -r requirements.txt
```
<!-- However, using the `Literal` type from the `typing` module makes the minimum supported version of Python 3.8.-->
Feel free to change the code to make it run on your version of Python in your machine

## Instructions to Run the Code

0. Clone this repository and change your current working directory.

```sh
git clone https://github.com/nrasadi/global-state-snapshot.git
cd global-state-snapshot
```

1. Instantiate at least the minimum number of branches of the bank.
Type the command below in different terminals.

```sh
python main.py -b
```

2. Instantiate inspector of the bank.
```bash
python main.py -i
```

Now, the code will run as a simulation.   
Type **`s`** in respective branches for snapshot.

### Logs

All transfer messages get logged into `logs/` directory.   
Each branch has its own log file. Same is true for the inspector.

### Next Runs
When you open enough terminals and run the program, it creates two directories:

- `logs/` (as mentioned above)
- `bank/`: public information of each branch, e.g., its id and ip address is kept here.

**Note:** When you want to run the program again, you need to remove these two directories.
It can be done using the following command:
```shell
python main.py -c
```
