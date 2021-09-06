import pickle
import threading
from threading import Thread
from datetime import datetime
import socket
import random

from commons import Constants
from bank import Bank


class Inspector:

    def __init__(self, address=None):

        self.address = "localhost" if address is None else address
        self.branches = []
        self.received_messages = []
        self.sent_messages = []
        self.lock = threading.Lock()
        self.n_global_snapshots = 0

        self.log_database = Constants.dir_root / "inspector.log"

        self.connect_to_branches()

        self._log("INSPECTOR LOG\n", in_file=True, stdio=False, file_mode="w")

    def connect_to_branches(self):

        self._log("Waiting for branches ...")

        while True:
            Bank.load_class_vars()
            if len(Bank.branches_public_details) == Bank.n_branches:
                self._log(f"All {Bank.n_branches} branches are open now. Resuming the process.")
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

                crspnd_msg = self.find_transfer_message(
                    message,
                    send_msg=True,
                    remove=True)

                if crspnd_msg:
                    crspnd_msg["send_time"] = message["send_time"]

            elif message["subject"] == "receive":
                self.sent_messages.append(message)

                crspnd_msg = self.find_transfer_message(
                    message,
                    send_msg=False,
                    remove=True)

                if crspnd_msg:
                    crspnd_msg["receive_time"] = message["receive_time"]

            elif message["subject"] == "global_snapshot":

                self.n_global_snapshots += 1
                total_balance = 0
                log_message = '\n===========================================================================\n' \
                              f'Global Snapshot #{self.n_global_snapshots}' \
                              f'     Request Time:{message["request_time"].strftime("%Y-%m-%d:%H:%M:%S")}' \
                              f'     Preparation Time:{message["preparation_time"].strftime("%Y-%m-%d:%H:%M:%S")}'
                for snapshot in message["local_snapshots"]:
                    log_message += f'\nBranch {snapshot["id"]:>2}: ' \
                                 f'Balance:{snapshot["balance"]:>9}{Bank.money_unit[0]} {Bank.money_unit[1]}' \
                                 f' - In Channels: {snapshot["in_channels"]:>9}{Bank.money_unit[0]}' \
                                 f' {Bank.money_unit[1]}'
                    total_balance += snapshot["balance"] + snapshot["in_channels"]
                log_message += f"\nTotal Balance: {total_balance}{Bank.money_unit[0]} {Bank.money_unit[1]}"
                log_message += '\n===========================================================================\n'

                self._log(log_message, in_file=True)

            if crspnd_msg:
                time_format = "%H:%M:%S"
                log_message = (
                    f'sender: {crspnd_msg["sender_id"]:>2} - '
                    f'send_time: '
                    f'{crspnd_msg["send_time"].strftime(time_format)}'
                    f' - amount:{crspnd_msg["amount"]:>9}'
                    f'{Bank.money_unit[0]} {Bank.money_unit[1]}'
                    f' - receiver:{crspnd_msg["receiver_id"]:>2}'
                    f' - receive_time: '
                    f'{crspnd_msg["receive_time"].strftime(time_format)}')

                self._log(log_message, in_file=True)
                crspnd_msg = None

    def find_transfer_message(self, message, send_msg: bool=False,remove=True):

        self.lock.acquire()

        corresponding_message = False
        query_list = self.sent_messages if send_msg else self.received_messages

        for i, prev_message in enumerate(query_list):

            if prev_message["amount"] == message["amount"] and \
                prev_message["sender_id"] == message["sender_id"] and \
                prev_message["receiver_id"] == message["receiver_id"]:

                if remove:
                    corresponding_message = query_list.pop(i)
                else:
                    corresponding_message = query_list[i]

                self.lock.release()
                return corresponding_message

        self.lock.release()
        return corresponding_message

    def run(self):

        threads = []
        for i in range(Bank.n_branches):
            threads.append(Thread(target=self.get_messages, args=(i, )))
            threads[-1].start()
        for i in range(Bank.n_branches):
            threads[i].join()


    def _log(self, message, stdio=True, in_file=False, file_mode="a+"):
        prefix = datetime.now().strftime("%Y-%m-%d:%H:%M:%S - ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message) + "\n")

    def _id_to_index(self, bid):
        return [i for i, branch in enumerate(self.branches) if branch["id"]==bid][0]
