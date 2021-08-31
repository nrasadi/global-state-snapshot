import json
import pickle
import random
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
from threading import Lock
from time import sleep
from typing import Any, List, Literal, Mapping, NoReturn, Optional, Tuple, Union

from commons import BaseClass, Constants, KBHit


class Bank(BaseClass):

    # Class Variables
    consts = Constants()

    if not consts.dir_bank.is_dir():
        consts.dir_bank.mkdir()

    bank_file = consts.dir_bank / "bank.json"
    money_unit = "Rupees"
    time_unit = 1  # Seconds
    n_branches = 2
    branches_public_details: List[Mapping[str, Any]] = []
    next_id = 0

    # Class Methods
    @staticmethod
    def load_class_vars() -> None:

        while True:
            try:
                if not Bank.bank_file.is_file():
                    Bank.next_id = 0
                    Bank.branches_public_details = []
                    return

                with open(Bank.bank_file, "r") as f:
                    bank_vars = json.load(f)

                Bank.next_id = len(bank_vars["branch_details"])
                Bank.branches_public_details = bank_vars["branch_details"]
                return
            except Exception:
                continue

    @staticmethod
    def save_class_vars() -> None:
        with open(Bank.bank_file, "w") as f:
            json.dump({"branch_details": Bank.branches_public_details}, f)

    def __init__(
        self,
        balance: int = 10_000_000,
        address: str = "localhost",
        port: Optional[int] = None,
        max_number_of_send: int = 1_000,
    ):

        self._log("Initiating ...")

        self.load_class_vars()

        self.lock = Lock()
        self.branches = []
        self.processes = []
        self.recv_queue = [Queue()] * self.n_branches
        self.id = self.next_id
        self.next_id += 1
        self.balance = balance
        self.max_n_send = max_number_of_send

        if not self.consts.dir_root.is_dir():
            self.consts.dir_root.mkdir()

        self.log_database = self.consts.dir_root / f"branch_{self.id}.log"

        self._log(f"Branch {self.id} started working.", in_file=True, file_mode="w")

        self.local_snapshots = []
        self.got_marker = False

        self.address = address
        self.port_base = port or (9_900 + self.id * 10)

        self.branches_public_details.append({"id": self.id, "address": self.address})
        self.save_class_vars()

        self._init_other_branches()
        self.inspector = {
            "port": 11_000 + self.id,
            "address": "localhost",
            "conn": None,
        }
        self._init_inspector()

        self._log(f"BRANCH {self.id} LOG\n", in_file=True, stdio=False, file_mode="w")

    def _init_other_branches(self) -> None:
        """
        Initiates information of other branches.
        Not all parts are completed.
        In this function the following information will be initiated:
            id: the of every branch
            port: the port that the other branch is listening through it.
            in_sock: the socket which this branch will listen to the branch with a specific id.
            out_conn: the socket that this branch can connect to the other branch with a specific id.
            address: the ip address of the other branch with a specific id.
        The following information cannot be initiated by this function:
            in_conn: the connection that the branch with a specific id will use to send a message to this branch.
            last_message: the last message which this branch received from the branch with a specific id.
        :return: None
        """
        self._log("Waiting for other branches ...")

        while True:
            self.load_class_vars()

            if len(self.branches_public_details) == self.n_branches:
                self._log(
                    f"All {self.n_branches} branches are open now. Resuming the process ..."
                )
                break

        for i in range(self.n_branches):
            if i != self.id:
                port = 9900 + self.id + i * 10

                curr_public_details = self.branches_public_details[i]

                in_sock = socket.socket()

                in_sock.bind((self.address, port))
                in_sock.listen(1)

                out_conn = socket.socket()

                self.branches.append(
                    {
                        "id": curr_public_details["id"],
                        "port": 9_900 + self.id * 10 + i,
                        "address": curr_public_details["address"],
                        "in_sock": in_sock,
                        "in_conn": None,
                        "out_conn": out_conn,
                        "last_message": Queue(),
                    }
                )


    def _init_inspector(self) -> None:
        """
        Initiates the connection between this branch and the inspector
        """
        self._log("Connecting to Inspector ...")

        while True:
            try:
                insp_sock = socket.socket()
                insp_sock.connect((self.inspector["address"], self.inspector["port"]))
                self.inspector["conn"] = insp_sock
                break
            except ConnectionRefusedError:
                insp_sock.close()

        self._log("Connected to Inspector.")

    def transfer(
        self, amount: int, receiver: Mapping[str, Any], show_error: bool = False
    ) -> Mapping[str, Union[bool, datetime]]:
        """
        Transfers amount to the receiver.
        :param amount: amount in integer.
        :param receiver: {"id": branch_id, "conn": out tcp connection}
        :param show_error: Whether log errors or not.
        :return: results. It is a dictionary containing the keys below:
            status: Boolean. Whether the transfer was successful or not.
            send_time: datetime object. The sending time.
        """
        # TODO: Write something like the below:
        if amount > self.balance:
            message = (
                "Transfer Failed: "
                f"The amount of money needed to transfer is more than asset of the branch #{self.id}."
            )
            if show_error:
                self._log(message, in_file=True)
            # raise message
            return {"status": False}

        message = {"subject": "transfer", "amount": amount}
        result = self._send_message(receiver["out_conn"], message)

        # check whether status == True or not
        if result["status"]:
            self.balance -= amount

        self._log(
            f"Branch {self.id}: {amount:4,} {self.money_unit} Transferred To the branch "
            f"{receiver['id']:2} (send_time: {result['send_time']})",
            in_file=True,
        )

        return result

    def _send_message(
        self, conn: socket.socket, message: Any
    ) -> Mapping[str, Union[bool, datetime]]:
        """
        Sends a message through the connection conn.
        :param conn: socket connection
        :param message: it can be everything
        :return: a dictionary with these keys: [status: bool, send_time: datetime object]
        """
        send_time = datetime.now()

        try:
            message = pickle.dumps(message)
            with self.lock:
                conn.sendall(message)
            status = True
        except Exception:
            status = False

        return {
            "status": status,  # True (succeeded) or False (failed)
            "send_time": send_time,
        }

    def do_common(self) -> None:
        """
        It does the common procedure of the program including transferring money and
        receiving messages from the other branches.
        All procedures are completely asynchronous.
        It calls two other methods per each thread:
            _do_common_transfer: transfer procedure
            _do_common_receive: receive messages. It will be called for each branch.
        :return: None
        """

        with ThreadPoolExecutor(thread_name_prefix="common_send_receive") as executor:
            for branch in self.branches:
                branch_id = branch["id"]
                executor.submit(self._do_common_transfer, branch_id)
                executor.submit(self._do_common_receive, branch_id)

    def _do_common_transfer(self, receiver_id: int) -> None:
        """
        Transfers an amount of money in range [1, 1000] to each branch with a probability of 0.3.
        After every transfer, a message will be sent to inspector.
        The Message Structure is like the example below:
            {"subject": "send", "sender_id": 2, "receiver_id": 1, "send_time": datetime.now(), "amount": 500}
        :param receiver_id: Id of the receiver information. The receiver itself is a dictionary that its keys are the same as the self.branch:
            [id:integer, "port": integer, "address": string,
            "in_sock": socket, "in_conn": input socket connection, "out_conn": output socket connection]
        :return:
        """
        receiver_id = self._id_to_index(receiver_id)
        receiver = self.branches[receiver_id]

        max_n_send = self.max_n_send
        while True:
            if max_n_send == 0:
                self._log(
                    f"Reached to the maximum number of sends ({self.max_n_send} messages per branch)",
                    in_file=True,
                )
                return

            sleep(self.time_unit)

            if random.random() <= 0.3:
                amount = random.randint(1, 1_000)
                result = self.transfer(amount, receiver, show_error=True)

                if result["status"]:
                    message = {
                        "subject": "send",
                        "sender_id": self.id,
                        "receiver_id": receiver["id"],
                        "send_time": result["send_time"],
                        "amount": amount,
                    }

                    self._send_message(conn=self.inspector["conn"], message=message)

                    max_n_send -= 1

    def _do_common_receive(self, sender_id: int) -> NoReturn:
        """
        Receives an amount of money from a branch with a specific id (in_soc_id).
        It also checks whether a snapshot request has been sent or not. If true, it will toggle on snapshot flag.
        :param sender_id: The id of sender information. The sender itself is a dictionary which its keys are same as the self.branch:
            [id:integer, "port": integer, "address": string,
            "in_sock": socket, "in_conn": input socket connection, "out_conn": output socket connection]
        :return:
        """

        with ThreadPoolExecutor(thread_name_prefix="recv_messages_th") as executor:
            executor.submit(self._recv_messages, sender_id)

            sender_index = self._id_to_index(sender_id)

            sender = self.branches[sender_index]

            while True:
                if self.recv_queue[sender_index].empty():
                    continue

                message = self.recv_queue[sender_index].get()

                # Add an intentional delay to simulate end-to-end connection latency.
                # sleep(self.time_unit * random.randint(1, 10))

                recv_time = datetime.now()
                amount = 0

                lower_subject = message["subject"].lower()

                if lower_subject == "transfer":
                    amount += message["amount"]
                    self.balance += amount
                    message_to_insp = {
                        "subject": "receive",
                        "amount": message["amount"],
                        "sender_id": sender["id"],
                        "receiver_id": self.id,
                        "receive_time": recv_time,
                    }

                    self._send_message(
                        conn=self.inspector["conn"], message=message_to_insp
                    )

                elif lower_subject == "snapshot":
                    self._log(
                        f"Branch {sender_id} has just sent its local snapshot.",
                        in_file=True,
                    )
                    self.local_snapshots.append(message)

                last_message = {
                    "recv_time": recv_time,
                    "amount": amount,
                    "subject": message["subject"],
                }
                if "initiator" in message:
                    last_message["initiator"] = message["initiator"]
                self.branches[sender_index]["last_message"].put(last_message)

    def _recv_messages(self, sender_id: int) -> NoReturn:
        sender_index = self._id_to_index(sender_id)
        sender = self.branches[sender_index]
        while True:
            pickled_data = sender["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)
            self.recv_queue[sender_index].put(message)

    def snapshot_process(self) -> NoReturn:
        while True:
            with ThreadPoolExecutor(thread_name_prefix="snapshot_process") as executor:
                executor.submit(self._init_snapshot)
                executor.submit(self._check_for_marker)

            self.got_marker = False
            self.local_snapshots = []

    def _get_local_snapshot(self, local: int, channels: List[int]) -> Mapping[str, Any]:
        try:
            on_the_fly = sum(channels)
            # Uncomment it if you want to get per channel amounts
            # on_the_fly = channels
        except TypeError:
            on_the_fly = 0

        return {
            "id": self.id,
            "subject": "snapshot",
            "balance": local,
            "on_the_fly": on_the_fly,
        }

    def _init_snapshot(self) -> None:

        kb = KBHit()
        self._log("TO GET A SNAPSHOT -> Type 's' \n")
        while True:
            if kb.kbhit():
                do_snapshot = kb.getch()

                if "s" in do_snapshot.lower():
                    self._log("Initiating a snapshot.", in_file=True)
                    kb.set_normal_term()
                    break

            if self.got_marker:
                kb.set_normal_term()
                return

        self.got_marker = True
        local, channels = self._do_snappy_things(initiator=self.id)
        request_time = datetime.now()

        local_snapshot = self._get_local_snapshot(local, channels)

        while True:
            if len(self.local_snapshots) == self.n_branches - 1:
                break

        self.local_snapshots.append(local_snapshot)

        preparation_time = datetime.now()

        self._create_global_snapshot(request_time, preparation_time)

        kb.set_normal_term()

    def _create_global_snapshot(
        self, request_time: datetime, preparation_time: datetime
    ) -> None:

        message = {
            "subject": "global_snapshot",
            "local_snapshots": [
                {
                    "id": local["id"],
                    "balance": local["balance"],
                    "in_channels": local["on_the_fly"],
                }
                for local in self.local_snapshots
            ],
            "request_time": request_time,
            "preparation_time": preparation_time,
        }

        self._send_message(conn=self.inspector["conn"], message=message)

    def _check_for_marker(self) -> Literal[0, 1]:

        branch_idx = 0
        while True:
            if self.got_marker:
                return 0

            if not self.branches[branch_idx]["last_message"].empty():
                last_message = self.branches[branch_idx]["last_message"].get()
                if last_message["subject"] == "marker":
                    sender_index = branch_idx
                    break

            branch_idx = (branch_idx + 1) % (self.n_branches - 1)

        self._log(
            f"Branch {self.branches[sender_index]['id']} has sent a snapshot request. "
            f"(Initiator: Branch {last_message['initiator']})",
            in_file=True,
        )

        local_snapshot = self._get_local_snapshot(
            *self._do_snappy_things(
                exclude_index=sender_index, initiator=last_message["initiator"]
            )
        )

        intitator_idx = self._id_to_index(last_message["initiator"])
        self._send_message(
            conn=self.branches[intitator_idx]["out_conn"], message=local_snapshot
        )

        self.got_marker = True
        return 1

    def _do_snappy_things(
        self, initiator: int, exclude_index: Optional[int] = None
    ) -> Tuple[int, list]:

        own_state = self.balance
        message = {"subject": "marker", "initiator": initiator}

        que = Queue()
        with ThreadPoolExecutor(thread_name_prefix="do_snappy_things") as executor:
            for branch_idx, branch in enumerate(self.branches):
                status = self._send_message(branch["out_conn"], message)
                if status["status"]:
                    self._log(f"Sent marker To {branch['id']}", in_file=True)

                if branch_idx != exclude_index:
                    executor.submit(
                        lambda: que.put(self._inspect_channel(branch["id"])),
                    )

        amounts_in_channels = list(que.queue)

        return own_state, amounts_in_channels

    def _inspect_channel(self, sender_id: int) -> int:

        sender_index = self._id_to_index(sender_id)

        amount = 0

        while True:
            if self.branches[sender_index]["last_message"].empty():
                continue

            last_message = self.branches[sender_index]["last_message"].get()
            last_subject = last_message["subject"]

            if last_subject == "marker":
                return amount
            elif last_subject == "transfer":
                try:
                    amount += int(last_message["amount"])
                except TypeError:
                    continue

    def run(self) -> None:
        with ThreadPoolExecutor(thread_name_prefix="bank_server_client") as executor:
            for branch in self.branches:
                branch_id = branch["id"]

                executor.submit(self._connect_to_branch, branch_id, "client")

                sleep(0.5)

                executor.submit(self._connect_to_branch, branch_id, "server")

        with ThreadPoolExecutor(thread_name_prefix="common_snapshot") as executor:
            executor.submit(self.do_common)
            executor.submit(self.snapshot_process)

    def _connect_to_branch(self, bid: int, mode: Literal["server", "client"]) -> None:
        """
        Establishes a connection between this branch and the branch with id 'bid'.
        :param bid: id of the other branch.
        :param mode: mode of connection establishment. It can be "server" or "client"
        :return: None
        """

        bid = self._id_to_index(bid)

        curr_branch = self.branches[bid]

        if mode == "server":
            (
                curr_branch["in_conn"],
                curr_branch["address"],
            ) = curr_branch["in_sock"].accept()

        else:
            curr_branch["out_conn"].connect(
                (curr_branch["address"], curr_branch["port"])
            )
