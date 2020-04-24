import pickle
from threading import Thread, Lock
from queue import Queue
from datetime import datetime
from time import sleep
import socket

import numpy as np
import json

from commons import Constants, KBHit


class Bank:

    # Class Variables
    consts = Constants()
    consts.dir_bank.mkdir() if not consts.dir_bank.is_dir() else None
    bank_file = consts.dir_bank / "bank.json"
    money_unit = "k", "Toomaan"
    time_unit = 0.5  # Seconds
    n_branches = 3
    branches_public_details = []
    next_id = 0

    # Class Methods
    @classmethod
    def load_class_vars(cls, ):


        while True:
            try:
                if not Bank.bank_file.is_file():
                    Bank.next_id = 0
                    Bank.branches_public_details = []
                    return

                with open(Bank.bank_file, "r") as f:
                    temp = f.read()
                bank_vars = json.loads(temp)

                Bank.next_id = len(bank_vars["branch_details"])
                Bank.branches_public_details = bank_vars["branch_details"]
                return
            except:
                continue

    @classmethod
    def save_class_vars(cls, ):

        with open(Bank.bank_file, "w") as f:
            json.dump({"branch_details":Bank.branches_public_details}, f)


    def __init__(self, balance=None, address=None, port=None, max_number_of_send=2):

        self._log("Initiating ...")

        Bank.load_class_vars()

        self.lock = Lock()
        self.branches = []
        self.processes = []
        self.recv_queue = [Queue() for _ in range(Bank.n_branches)]
        self.id = Bank.next_id
        Bank.next_id += 1
        self.balance = 10000000 if balance is None else balance
        self.max_n_send = max_number_of_send

        Bank.consts.dir_root.mkdir() if not Bank.consts.dir_root.is_dir() else None
        self.log_database = Bank.consts.dir_root / f"branch_{self.id}.log"

        self._log(f"Branch {self.id} started working.", in_file=True, file_mode='w')

        self.do_snapshot = -1
        self.local_snapshots = []
        self.got_marker = False

        self.address = "localhost" if address is None else address
        self.port_base = 9900 + self.id * 10 if port is None else port

        Bank.branches_public_details.append({"id": self.id, "address": self.address})
        Bank.save_class_vars()

        self._init_other_branches()
        self.inspector = {"port": 11000 + self.id, "address": "localhost", "conn": None}
        self._init_inspector()

        self._log(f"BRANCH {self.id} LOG\n", in_file=True, stdio=False, file_mode="w")

    def _init_other_branches(self):
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
            Bank.load_class_vars()

            if len(Bank.branches_public_details) == Bank.n_branches:
                self._log(f"All {Bank.n_branches} branches are open now. Resuming the process ...")
                break

        for i in range(Bank.n_branches):
            if i != self.id:
                port =  9900 + self.id + i * 10
                self.branches.append({
                    "id": Bank.branches_public_details[i]["id"],
                    "port": 9900 + self.id  * 10 + i,
                    "address": Bank.branches_public_details[i]["address"],
                    "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    "in_conn": None,
                    "out_conn": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    "last_message": Queue()
                })
                self.branches[-1]["in_sock"].bind((self.address, port))
                self.branches[-1]["in_sock"].listen(1)

    def _init_inspector(self):
        """
        Initiates the connection between this branch and the inspector
        :return:
        """
        self._log("Connecting to Inspector ...")

        while True:
            try:
                insp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                insp_sock.connect((self.inspector["address"], self.inspector["port"]))
                self.inspector["conn"] = insp_sock
                break
            except:
                continue

        self._log("Connected to Inspector.")

    def transfer(self, amount: int, receiver, show_error: bool=False):
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
            message =f"Transfer Failed: " \
                     f"The amount of money needed to transfer is more than asset of the branch #{self.id}."
            if show_error:
                self._log(message, in_file=True)
            # raise message
            return {"status": False}

        message = {"subject":"transfer", "amount": amount}
        result = self._send_message(receiver["out_conn"], message)

        # check whether status == True or not
        if result["status"]:
            self.balance -= amount

        self._log(f"Branch {self.id}: {amount:>4}{Bank.money_unit[0]} {Bank.money_unit[1]} Transferred TO the branch "
                  f"{receiver['id']:>2}. (send_time:{result['send_time']})", in_file=True)

        return result

    def _send_message(self, conn, message):
        """
        Sends a message through the connection conn.
        :param conn: socket connection
        :param message: it can be everything
        :return: a dictionary with these keys: [status: Bool, send_time: datetime object]
        """
        self.lock.acquire()
        send_time = datetime.now()

        try:
            message = pickle.dumps(message)
            conn.sendall(message)
            status = True
        except:
            status = False

        self.lock.release()

        return {
            "status": status, # True (succeeded) or False (failed)
            "send_time": send_time,
        }

    def do_common(self):
        """
        It does the common procedure of the program including transferring money and
        receiving messages from the other branches.
        All procedures are completely asynchronous.
        It calls two other methods per each thread:
            _do_common_transfer: transfer procedure
            _do_common_receive: receive messages. It will be called for each branch.
        :return: None
        """

        receive = []
        send = []
        for branch in self.branches:

            send.append(Thread(target=self._do_common_transfer, args=(branch["id"], ),
                               name=f"common_transfer_to{branch['id']}_th"))

            receive.append(Thread(target=self._do_common_receive, args=(branch["id"],),
                                  name=f"common_receive_from{branch['id']}_th"))
        for th in zip(receive, send):
            th[0].start()
            th[1].start()

    def _do_common_transfer(self, receiver_id):
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
                self._log(f"Reached to the maximum number of sends ({self.max_n_send} messages per branch)",
                          in_file=True)
                return

            sleep(Bank.time_unit)

            if np.random.random() <= 0.3:
                amount = np.random.randint(1, 1001)
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
                    print("max_n_send:")

    def _do_common_receive(self, sender_id):
        """
        Receives an amount of money from a branch with a specific id (in_soc_id).
        It also checks whether a snapshot request has been sent or not. If true, it will toggle on snapshot flag.
        :param sender_id: The id of sender information. The sender itself is a dictionary which its keys are same as the self.branch:
            [id:integer, "port": integer, "address": string,
            "in_sock": socket, "in_conn": input socket connection, "out_conn": output socket connection]
        :return:
        """

        recv_messages_th = Thread(target=self._recv_messages, args=(sender_id,), name="recv_messages_th")
        recv_messages_th.start()

        sender_index = self._id_to_index(sender_id)

        sender = self.branches[sender_index]

        while True:
            if not self.recv_queue[sender_index].empty():
                message = self.recv_queue[sender_index].get()
                # Add an intentional delay to simulate end-to-end connection latency.
                sleep(Bank.time_unit * np.random.randint(1, 11))
            else:
                continue

            recv_time = datetime.now()
            amount = 0
            if message["subject"].lower() == "transfer":
                amount += message["amount"]
                self.balance += amount
                message_to_insp = {
                    "subject": "receive",
                    "amount": message["amount"],
                    "sender_id": sender["id"],
                    "receiver_id": self.id,
                    "receive_time": recv_time
                }

                self._send_message(conn=self.inspector["conn"], message=message_to_insp)

            elif message["subject"].lower() == "snapshot":
                self._log(f"Branch {sender_id} has just sent its local snapshot.", in_file=True)
                self.local_snapshots.append(message)

            last_message = {"recv_time": recv_time, "amount": amount, "subject": message["subject"]}
            if "initiator" in message.keys():
                last_message["initiator"] = message["initiator"]
            self.branches[sender_index]["last_message"].put(last_message)

    def _recv_messages(self, sender_id):

        sender_index = self._id_to_index(sender_id)
        sender = self.branches[sender_index]
        while True:
            pickled_data = sender["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)
            self.recv_queue[sender_index].put(message)


    def snapshot_process(self):

        while True:
            init_snapshot_th = Thread(target=self._init_snapshot, name="init_snapshot_th")
            check_for_marker_th = Thread(target=self._check_for_marker, name="check_for_marker_th")
            init_snapshot_th.start()
            check_for_marker_th.start()

            check_for_marker_th.join()
            init_snapshot_th.join()

            self.got_marker = False
            self.do_snapshot = -1
            self.local_snapshots = []

    def _init_snapshot(self):

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

        try:
            on_the_fly = sum(channels)
        except TypeError:
            on_the_fly = 0

        local_snapshot = {
            "id": self.id,
            "subject": "snapshot",
            "balance": local,
            "on_the_fly": on_the_fly
        }

        while True:
            if len(self.local_snapshots) == Bank.n_branches - 1:
                break

        self.local_snapshots.append(local_snapshot)

        preparation_time = datetime.now()

        self._create_global_snapshot(request_time, preparation_time)

        kb.set_normal_term()

    def _create_global_snapshot(self, request_time, preparation_time):

        message = {
            "subject": "global_snapshot",
            "local_snapshots": [{"id": local["id"],
                                 "balance": local["balance"],
                                 "in_channels": local["on_the_fly"]}
                                for local in self.local_snapshots],
            "request_time": request_time,
            "preparation_time": preparation_time
        }

        self._send_message(conn=self.inspector["conn"], message=message)



    def _check_for_marker(self):

        branch_idx = 0
        while True:

            if self.got_marker:
                return 0

            if self.branches[branch_idx]["last_message"].empty():
                branch_idx = (branch_idx + 1) % (Bank.n_branches - 1)
                continue

            last_message = self.branches[branch_idx]["last_message"].get()
            if last_message["subject"] == "marker":
                sender_index = branch_idx
                break

            branch_idx = (branch_idx + 1) % (Bank.n_branches - 1)

        self._log(f"Branch {self.branches[sender_index]['id']} has sent a snapshot request. "
                  f"(Initiator: Branch {last_message['initiator']})", in_file=True)

        local, channels = self._do_snappy_things(exclude_index=sender_index, initiator=last_message['initiator'])

        try:
            on_the_fly = sum(channels)
        except TypeError:
            on_the_fly = 0

        local_snapshot = {
            "id": self.id,
            "subject": "snapshot",
            "balance": local,
            "on_the_fly": on_the_fly
        }
        intitator_idx = self._id_to_index(last_message["initiator"])
        self._send_message(conn=self.branches[intitator_idx]["out_conn"], message=local_snapshot)

        self.got_marker = True
        return 1

    def _do_snappy_things(self, initiator, exclude_index=None ):

        own_state = self.balance
        message = {"subject": "marker", "initiator": initiator }

        que = Queue()
        threads = []
        for branch_idx, branch in enumerate(self.branches):
            status = self._send_message(branch["out_conn"], message)
            if status["status"]:
                self._log(f"Sent marker TO {branch['id']}", in_file=True)

            if branch_idx != exclude_index:
                threads.append(Thread(target=lambda q, arg1: q.put(self._inspect_channel(arg1)),
                                      args=(que, branch["id"],)))
                threads[-1].start()

        for th in threads:
            th.join()

        amounts_in_channels = list(que.queue)

        return own_state, amounts_in_channels

    def _inspect_channel(self, sender_id):

        sender_index = self._id_to_index(sender_id)

        amount = 0

        while True:
            try:
                if self.branches[sender_index]["last_message"].empty():
                    continue

                last_message = self.branches[sender_index]["last_message"].get()

                if last_message["subject"] == "marker":
                    return amount
                elif last_message["subject"] == "transfer":
                    amount += last_message["amount"]

            except TypeError:
                continue

    def run(self):

        threads = []
        for branch in self.branches:

            threads.append(Thread(target=self._connect_to_branch, args=(branch["id"], "client")))
            threads[-1].start()

            sleep(0.5)

            threads.append(Thread(target=self._connect_to_branch, args=(branch["id"], "server")))
            threads[-1].start()

        for th in threads:
            th.join()

        t1 = Thread(target=self.do_common, name="do_common_th")
        t2 = Thread(target=self.snapshot_process, name="snapshot_process_th")
        t1.start()
        t2.start()

    def _connect_to_branch(self, bid: int, mode: str):
        """
        Establishes a connection between this branch and the branch with id 'bid'.
        :param bid: id of the other branch.
        :param mode: mode of connection establishment. It can be "server" of "client"
        :return: None
        """

        bid = self._id_to_index(bid)

        if mode == "server":
            self.branches[bid]["in_conn"], self.branches[bid]["address"] = self.branches[bid]["in_sock"].accept()

        elif mode == "client":
            while True:
                address, port = self.branches[bid]["address"], self.branches[bid]["port"]
                self.branches[bid]["out_conn"].connect((address, port))
                break

    def _log(self, message, stdio=True, in_file=False, file_mode="a+"):
        prefix = datetime.now().strftime("%Y%m%d-%H:%M:%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message) + "\n")

    def _id_to_index(self, bid):
        return [i for i, branch in enumerate(self.branches) if branch["id"]==bid][0]