import pickle
import socket
import threading
from threading import Thread

from bank import Bank
from commons import BaseClass, Constants


class Inspector(BaseClass):
    def __init__(self, address="localhost"):

        self.get_config()

        self.address = self.inspctr_confs["address"]
        if self.address is None:
            self.address = address

        self.n_branches = len(self.brnch_confs)

        self.branches = []
        self.received_messages = []
        self.sent_messages = []
        self.lock = threading.Lock()
        self.n_global_snapshots = 0

        self.log_database = Constants.dir_logs / self.inspctr_confs["log_file"]

        self.connect_to_branches()

        self._log("INSPECTOR LOG\n", in_file=True, stdio=False, file_mode="w")

    def connect_to_branches(self):

        self._log("Waiting for branches ...")

        while True:
            Bank.load_class_vars()
            if len(Bank.branches_public_details) == self.n_branches:
                self._log(
                    f"All {self.n_branches} branches are open now. Resuming the process."
                )
                break

        for i in range(self.n_branches):
            self.branches.append(
                {
                    "id": Bank.branches_public_details[i]["id"],
                    "address": Bank.branches_public_details[i]["address"],
                    "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    "in_conn": None,
                }
            )

            port_in = self.inspctr_confs["port_base_in"] + self.branches[-1]["id"]
            self.branches[-1]["in_sock"].bind((self.address, port_in))
            self.branches[-1]["in_sock"].listen(1)

    def get_messages(self, bid):

        bid = self._id_to_index(bid)

        self.branches[bid]["in_conn"], self.branches[bid]["address"] = self.branches[
            bid
        ]["in_sock"].accept()

        time_format = "%Y-%m-%d:%H:%M:%S"
        c_sign = self.bank_confs["currency"]["symbol"]
        c_unit = self.bank_confs["currency"]["unit"]
        s_place = self.bank_confs["currency"]["placement"]
        sign_before = f'{c_sign if s_place == "before" else ""}'
        sign_after = f'{c_sign if s_place == "after" else ""}'
        merged_unit_sign_after = c_unit + " " + sign_after

        while True:
            pickled_data = self.branches[bid]["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)

            # print("message:", message)

            if message["subject"] == "send":
                self.received_messages.append(message)

                crspnd_msg = self.find_transfer_message(
                    message, send_msg=True, remove=True
                )

                if crspnd_msg:
                    crspnd_msg["send_time"] = message["send_time"]

            elif message["subject"] == "receive":
                self.sent_messages.append(message)

                crspnd_msg = self.find_transfer_message(
                    message, send_msg=False, remove=True
                )

                if crspnd_msg:
                    crspnd_msg["receive_time"] = message["receive_time"]

            elif message["subject"] == "global_snapshot":

                self.n_global_snapshots += 1
                total_balance = 0
                log_message = (
                    "\n=============================================="
                    "=============================\n"
                    f"Global Snapshot #{self.n_global_snapshots}"
                    f"\tRequest Time:"
                    f'{message["request_time"].strftime(time_format)}'
                    f"\tPreparation Time:"
                    f'{message["preparation_time"].strftime(time_format)}'
                )

                for snapshot in message["local_snapshots"]:
                    log_message += (
                        f'\nBranch {snapshot["id"]:>2}: Balance:'
                        f"{sign_before}"
                        f'{snapshot["balance"]}'
                        # f'{c_unit:<9} '
                        # f'{sign_after} '
                        f"{merged_unit_sign_after:<8}"
                        f"- In Channels: "
                        f"{sign_before}"
                        f'{snapshot["in_channels"]}'
                        # f'{c_unit:<9} '
                        # f'{sign_after}')
                        f"{merged_unit_sign_after:<6}"
                    )

                    total_balance += snapshot["balance"] + snapshot["in_channels"]

                log_message += (
                    f"\nTotal Balance: "
                    f"{sign_before}"
                    f"{total_balance}"
                    # f'{c_unit:<9} '
                    # f'{sign_after}')
                    f"{merged_unit_sign_after:<9}"
                )

                log_message += (
                    "\n============================================="
                    "==============================\n"
                )

                self._log(log_message, in_file=True)

            if crspnd_msg:
                time_format = "%H:%M:%S"
                log_message = (
                    f'sender: {crspnd_msg["sender_id"]:>2} - '
                    f"send_time: "
                    f'{crspnd_msg["send_time"].strftime(time_format)}'
                    f" - amount:"
                    f"{sign_before}"
                    f'{crspnd_msg["amount"]}'
                    # f'{c_unit:<9} '
                    # f'{sign_after}'
                    f"{merged_unit_sign_after:<4}"
                    f' - receiver:{crspnd_msg["receiver_id"]:>2}'
                    f" - receive_time: "
                    f'{crspnd_msg["receive_time"].strftime(time_format)}'
                )

                self._log(log_message, in_file=True)
                crspnd_msg = None

    def find_transfer_message(self, message, send_msg: bool = False, remove=True):

        self.lock.acquire()

        corresponding_message = False
        query_list = self.sent_messages if send_msg else self.received_messages

        for i, prev_message in enumerate(query_list):

            if (
                prev_message["amount"] == message["amount"]
                and prev_message["sender_id"] == message["sender_id"]
                and prev_message["receiver_id"] == message["receiver_id"]
            ):

                corresponding_message = query_list.pop(i) if remove else query_list[i]
                break

        self.lock.release()
        return corresponding_message

    def run(self):

        threads = []
        for i in range(self.n_branches):
            threads.append(Thread(target=self.get_messages, args=(i,)))
            threads[-1].start()
        for i in range(self.n_branches):
            threads[i].join()
