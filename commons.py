import numpy as np
from datetime import datetime
from time import sleep
from multiprocessing import Pool
import matplotlib.pyplot as plt
import socket as soc
from pathlib import Path
import json
import asyncio


class Constants:
    dir_root = Path("/media/navid74/DiscD/RELATED2UNI/0MSc/3982/AdvancedOS/Project/T01")


class Bank:

    # Class Variables
    next_id = 0
    consts = Constants()
    money_unit = 1000, "Toomaan"
    time_unit = 0.5 # Seconds

    def __init__(self, balance=None):

        self.id = Bank.next_id
        Bank.next_id += 1
        self.balance = 10000000 if balance is None else balance
        self.log_database = Bank.consts.dir_root / f"branch_{self.id}.log"
        self._log(f"The branch {self.id} started working.", in_file=True, file_mode='w')


    def transfer(self, amount, receiver):

        # TODO: Write something like the below:
        if amount > self.balance:
            message =f"The amount of money needed to transfer is more than asset of the branch #{self.id}."
            self._log(message, in_file=True)
            raise message

        result = self._send_message(receiver, json.dumps({"transfer": amount, "marker": False}))

        if "success" in result.lower():
          self.balance -= amount


        self._log(f"Branch {self.id}: {amount * Bank.money_unit[0]}{Bank.money_unit[1]} Transferred TO the branch "
                  f"{receiver.id}.")

        pass

    def snapshot(self):

        # Record the Own Process State
        own_balance = self.balance

        # Send Marker Messages
        for receiver in All_Receivers:
            self._send_message(receiver, json.dumps({"marker": True, "transfer": 0}))

        # Record Incoming Messages





        pass

    def _send_message(self, receiver, message):

        # TODO: Socket Procedure

        sleep(np.random.random(1, 11))
        # return successful/failed (1 or 0)
        pass


    def report(self, message, receiver):
        """

        :param message: contains: self.id, send_time, amount, receiver.id, receive_time
        :param receiver:
        :return:
        """
        # Connect to receiver
        # if connection:
        #   result = transfer(amount)
        # if result == Successful:
        #   send message to receiver

    def do_common(self):

        do = True
        while do:
            sleep(0.5)

            receiver_id = np.random.randint(0, 4)
            receiver = get_receiver(receiver_id)
            amount = np.random.randint(1, 1001)
            self.transfer(amount, receiver)

    def do_snapshot(self):

        do = True
        while do:
            inp = input("Initiate a Snapshot? [Enter]")
            print("Initiating Snapshot...")
            self.snapshot()
            # TODO : snapshot precedure
            pass

    def run(self):

        pool = Pool(2)
        pool.apply_async(self.do_common())
        pool.apply_async(self.do_snapshot())




    def _log(self, message, stdio=True, in_file=False, file_mode="r+"):
        prefix = datetime.now().strftime("%Y-%m-%d-%H-%M-%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message))


