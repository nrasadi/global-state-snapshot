import pickle
import socket
import threading
from datetime import datetime
from threading import Thread

from bank import Bank
from commons import Constants


class Inspector:
    def __init__(self, address="localhost"):

        self.address = address
        self.branches = []
        self.received_messages = []
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
                self._log(
                    f"All {Bank.n_branches} branches are open now. "
                    "Resuming the process."
                )
                break

        for i in range(Bank.n_branches):
            self.branches.append(
                {
                    "id": Bank.branches_public_details[i]["id"],
                    "address": Bank.branches_public_details[i]["address"],
                    "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    "in_conn": None,
                }
            )
            self.branches[-1]["in_sock"].bind(
                (self.address, 11000 + self.branches[-1]["id"])
            )
            self.branches[-1]["in_sock"].listen(1)

    def get_messages(self, bid):

        bid = self._id_to_index(bid)

        self.branches[bid]["in_conn"], self.branches[bid]["address"] = self.branches[
            bid
        ]["in_sock"].accept()

        while True:
            pickled_data = self.branches[bid]["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)

            # print("message:", message)

            if message["subject"] == "send":
                self.received_messages.append(message)

            elif message["subject"] == "receive":
                send_message = self.find_transfer_message(message, remove=True)
                if send_message:
                    log_message = (
                        f'sender:{send_message["sender_id"]:>2} '
                        f'- send_time:{send_message["send_time"].strftime("%H:%M:%S ")}'
                        f'- amount:{send_message["amount"]:>9}{Bank.money_unit[0]} {Bank.money_unit[1]} '
                        f'- receiver:{send_message["receiver_id"]:>2}'
                        f'- receive_time:{message["receive_time"].strftime("%H:%M:%S ")}'
                    )
                    self._log(log_message, in_file=True)

            elif message["subject"] == "global_snapshot":

                self.n_global_snapshots += 1
                total_balance = 0
                log_message = (
                    "\n===========================================================================\n"
                    f"Global Snapshot #{self.n_global_snapshots}"
                    f'     Request Time:{message["request_time"].strftime("%Y-%m-%d:%H:%M:%S")}'
                    f'     Preparation Time:{message["preparation_time"].strftime("%Y-%m-%d:%H:%M:%S")}'
                )
                for snapshot in message["local_snapshots"]:
                    log_message += (
                        f'\nBranch {snapshot["id"]:>2}: '
                        f'Balance:{snapshot["balance"]:>9}{Bank.money_unit[0]} {Bank.money_unit[1]}'
                        f' - In Channels: {snapshot["in_channels"]:>9}{Bank.money_unit[0]}'
                        f" {Bank.money_unit[1]}"
                    )
                    total_balance += snapshot["balance"] + snapshot["in_channels"]
                log_message += f"\nTotal Balance: {total_balance}{Bank.money_unit[0]} {Bank.money_unit[1]}"
                log_message += "\n===========================================================================\n"

                self._log(log_message, in_file=True)

    def find_transfer_message(self, message, remove=True):

        self.lock.acquire()
        send_message = False
        for i, prev_message in enumerate(self.received_messages):
            if (
                prev_message["amount"] == message["amount"]
                and prev_message["sender_id"] == message["sender_id"]
                and prev_message["receiver_id"] == message["receiver_id"]
            ):

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
            threads.append(Thread(target=self.get_messages, args=(i,)))
            threads[-1].start()
        for i in range(Bank.n_branches):
            threads[i].join()

    def _log(self, message, stdio=True, in_file=False, file_mode="a+"):
        prefix = datetime.now().strftime("%Y-%m-%d:%H:%M:%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message) + "\n")

    def _id_to_index(self, bid):
        for i, branch in enumerate(self.branches):
            if branch["_id"] == bid:
                return i
