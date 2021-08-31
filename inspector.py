import pickle
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Any, List, Mapping, NoReturn, Optional

from bank import Bank
from commons import BaseClass, Constants


class Inspector(BaseClass):
    def __init__(self, address: str = "localhost"):

        self.address = address
        self.branches: List[Mapping[str, Any]] = []
        self.received_messages: List[Mapping[str, Any]] = []
        self.lock = Lock()
        self.n_global_snapshots = 0

        self.log_database = Constants.dir_root / "inspector.log"

        self.connect_to_branches()

        self._log("INSPECTOR LOG\n", in_file=True, stdio=False, file_mode="w")

    def connect_to_branches(self) -> None:

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
            soc = socket.socket()

            id = Bank.branches_public_details[i]["id"]

            soc.bind((self.address, 11_000 + id))
            soc.listen(1)

            self.branches.append(
                {
                    "id": id,
                    "address": Bank.branches_public_details[i]["address"],
                    "in_sock": soc,
                    "in_conn": None,
                }
            )

    def get_messages(self, bid: int) -> NoReturn:

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
                    time_format = "%H:%M:%S"
                    receive_time = message["receive_time"].strftime(time_format)
                    send_time = send_message["send_time"].strftime(time_format)

                    log_message = (
                        f'sender: {send_message["sender_id"]:2} '
                        f"- send_time: {send_time}"
                        f'- amount: {send_message["amount"]:9,} {Bank.money_unit} '
                        f'- receiver: {send_message["receiver_id"]:2}'
                        f"- receive_time: {receive_time}"
                    )
                    self._log(log_message, in_file=True)

            elif message["subject"] == "global_snapshot":
                self.n_global_snapshots += 1
                total_balance: int = 0

                time_format = r"%Y-%m-%d:%H:%M:%S"

                request_time = message["request_time"].strftime(time_format)
                preparation_time = message["preparation_time"].strftime(time_format)

                log_message = (
                    "\n==================================="
                    "========================================\n"
                    f"Global Snapshot #{self.n_global_snapshots}"
                    f"\tRequest Time: {request_time}"
                    f"\tPreparation Time: {preparation_time}"
                )
                for snapshot in message["local_snapshots"]:

                    balance: int = snapshot["balance"]
                    in_channels: int = snapshot["in_channels"]

                    log_message += (
                        f'\nBranch {snapshot["id"]:2}: '
                        f"Balance: {balance:9,} {Bank.money_unit}"
                        f" - In Channels: {in_channels:9}"
                        f" {Bank.money_unit}"
                    )
                    total_balance += balance + in_channels
                log_message += (
                    f"\nTotal Balance: {total_balance:,} {Bank.money_unit}"
                    "\n======================================"
                    "=====================================\n"
                )

                self._log(log_message, in_file=True)

    def find_transfer_message(
        self, message: Mapping[str, Any], remove: bool = True
    ) -> Optional[Mapping[str, Any]]:

        for i, prev_message in enumerate(self.received_messages):
            if (
                prev_message["amount"] == message["amount"]
                and prev_message["sender_id"] == message["sender_id"]
                and prev_message["receiver_id"] == message["receiver_id"]
            ):
                if remove:
                    with self.lock:
                        del self.received_messages[i]
                return prev_message

    def run(self) -> None:
        with ThreadPoolExecutor() as executor:
            executor.map(self.get_messages, range(Bank.n_branches))
