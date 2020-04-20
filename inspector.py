import pickle
import threading
from threading import Thread
from multiprocessing.pool import ThreadPool
from datetime import datetime
from time import sleep
from multiprocessing import Pool
import socket

import numpy as np
import matplotlib.pyplot as plt
import socket
from pathlib import Path

from .commons import Bank, Constants


class Inspector:

    def __init__(self, address):

        self.address = "localhost" if address is None else address
        self.ports = np.arange(Bank.n_branches) + 11000
        self.branches = []
        self.received_messages = []


    def connect_to_branches(self):
        for i in Bank.n_branches:
            self.branches.append({
                "id": Bank.branches_public_details[i]["id"],
                "address": Bank.branches_public_details[i]["address"],
                "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                "in_conn": None
            })
            self.branches[-1]["in_sock"].bind((self.address, 11000 + self.branches[-1]["id"]))
            self.branches[-1]["in_sock"].listen(1)

        pass

    def get_messages(self, bid):

        self.branches[bid]["in_conn"], self.branches[bid]["address"] = self.branches[bid]["in_sock"].accept()

        while True:
            pickled_data = self.branches[bid]["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)

            if self.received_messages


        pass

    def find_transfer_message(self, message, remove=True):

        for i, recv_msg in self.received_messages:
            if recv_msg["amount"] == message["amount"] and recv_msg["sender_id"] == message["receiver_id"] and recv_msg[
                "receiver_id"] == message["sender_id"]:

                if remove:
                    self.received_messages[i]:




    def show_transfers(self):

        pass

    def show_snapshots(self):

        pass

    def run(self):

        threads = []
        for branch in self.branches:
            threads.append(Thread(target=self.get_messages, args=(branch["id"], )))
            threads[-1].start()
        for i in range(Bank.n_branches):
            threads[i].join()

