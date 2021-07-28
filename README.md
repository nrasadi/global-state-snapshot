# Chandy and Lamport's Global Snapshot Algorithm

It is the implementation of [Chandy and Lamport's snapshot algorithm](https://lamport.azurewebsites.net/pubs/chandy.pdf) with distributed banking as an example.
In this project, we instantiate four bank branches before the inspector records the global state, by default.
We can change it by altering the `n_branches` variable of the `Bank` class in `bank.py`.
The transaction is performed every 0.5 seconds by default. We can change it too by changing the `time_unit` variable in the same class.
A branch can initiate a snapshot at any time by pressing **s** in the terminal.

## Platform

Although through testing of the project was done in Arch Linux. It should run fine in other operating systems too.

## Dependencies

The project uses the libraries provided on the default installation of python.
There aren't dependencies that you have to install via your package manager.
However, using the `Literal` type from the `typing` module makes the minimum supported version of Python 3.8.
Feel free to change the code to make it run on your version of python in your machine.

## Instructions to Run Code

1. Instantiate at least the minimum number of branches of the bank.
Type the command below in different terminals.

```sh
python main.py -b 
```

2. Instantiate inspector of the bank. 
```bash
python main.py -i
```

Now, the code will run as a simulation. Type 's' in respective banks for snapshot.
