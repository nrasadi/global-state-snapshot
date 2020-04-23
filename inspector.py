import pickle
import threading
from threading import Thread
from datetime import datetime
import socket
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import numpy as np
import matplotlib.pyplot as plt

from commons import Bank, Constants


class Inspector:

    def __init__(self, address=None):

        self.address = "localhost" if address is None else address
        self.ports = np.arange(Bank.n_branches) + 11000
        self.branches = []
        self.received_messages = []
        self.lock = threading.Lock()

        self.log_database = Constants.dir_root / "inspector.log"
        self.log_database.mkdir(parents=True) if not self.log_database.is_dir() else None

        self.connect_to_branches()

    def connect_to_branches(self):

        self._log("All branches are not started working yet. Waiting for the others ...")

        while True:
            Bank.load_class_vars()
            if len(Bank.branches_public_details) == Bank.n_branches:
                self._log(f"All {Bank.n_branches} branches are open now. Resuming the procedure ...")
                break

        for i in range(Bank.n_branches):
            self.branches.append({
                "id": Bank.branches_public_details[i]["id"],
                "address": Bank.branches_public_details[i]["address"],
                "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                "in_conn": None
            })
            self.branches[-1]["in_sock"].bind((self.address, 11000 + self.branches[-1]["id"]))
            self.branches[-1]["in_sock"].listen(1)

    def get_messages(self, bid):

        bid = self._id_to_index(bid)

        self.branches[bid]["in_conn"], self.branches[bid]["address"] = self.branches[bid]["in_sock"].accept()

        while True:
            pickled_data = self.branches[bid]["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)

            # print("message:", message)

            if message["subject"] == "send":
                self.received_messages.append(message)

            elif message["subject"] == "receive":
                send_message = self.find_transfer_message(message, remove=True)
                if send_message:
                    log_message = f'sender:{send_message["sender_id"]} ' \
                                  f'- send_time:{send_message["send_time"].strftime("%H:%M:%S ")}' \
                                  f'- amount:{send_message["amount"] * Bank.money_unit[0]}{Bank.money_unit[1]}' \
                                  f'- receiver:{send_message["receiver_id"]}' \
                                  f'- receive_time:{message["receive_time"].strftime("%H:%M:%S ")}'
                    self._log(log_message)

            elif message["subject"] == "global_snapshot":


                log_message = '\n============================================================\n' \
                              'Global Snapshot:'
                for snapshot in message["local_snapshots"]:
                              log_message += f'\nBranch {snapshot["id"]}: ' \
                                             f'Balance:{snapshot["balance"] * Bank.money_unit[0]}{Bank.money_unit[1]}' \
                                             f'- In Channels: {snapshot["in_channels"] * Bank.money_unit[0]}' \
                                             f'{Bank.money_unit[1]}'
                log_message += '\n============================================================\n'

                self._log(log_message)

    def find_transfer_message(self, message, remove=True):

        self.lock.acquire()
        send_message = False
        for i, prev_message in enumerate(self.received_messages):
            if prev_message["amount"] == message["amount"] and \
                            prev_message["sender_id"] == message["sender_id"] and \
                            prev_message["receiver_id"] == message["receiver_id"]:

                if remove:
                    send_message = self.received_messages.pop(i)
                else:
                    send_message = self.received_messages[i]

                self.lock.release()
                return send_message

        self.lock.release()
        return send_message

    def run(self):

        threads = []
        for i in range(Bank.n_branches):
            threads.append(Thread(target=self.get_messages, args=(i, )))
            threads[-1].start()
        for i in range(Bank.n_branches):
            threads[i].join()


    def _log(self, message, stdio=True, in_file=False, file_mode="r+"):
        prefix = datetime.now().strftime("%Y-%m-%d:%H:%M:%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message))

    def _id_to_index(self, bid):
        return [i for i, branch in enumerate(self.branches) if branch["id"]==bid][0]