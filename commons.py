import pickle
import threading
from threading import Thread
from queue import Queue
from multiprocessing.pool import ThreadPool
from datetime import datetime
from time import sleep
from multiprocessing import Pool
import socket

from pathlib import Path
import numpy as np
import json


class Constants:
    dir_root = Path("logs/")
    dir_bank = Path("bank/")


class Bank:


    # Class Variables
    consts = Constants()
    consts.dir_bank.mkdir() if not consts.dir_bank.is_dir() else None
    bank_file = consts.dir_bank / "bank.json"
    money_unit = 1000, "Toomaan"
    time_unit = 0.5  # Seconds
    n_branches = 2
    branches_public_details = []
    next_id = 0

    @classmethod
    def load_class_vars(cls, ):


        while True:
            try:
                if not Bank.bank_file.is_file():
                    Bank.next_id = 0
                    Bank.branches_public_details = []
                    return

                # f = open(Bank.bank_file, "r")
                # bank_vars = json.load(f)
                with open(Bank.bank_file, "r") as f:
                    temp = f.read()
                bank_vars = json.loads(temp)

                Bank.next_id = len(bank_vars["branch_details"])
                Bank.branches_public_details = bank_vars["branch_details"]
                return
            except:
                print("Error in loading bank file")
                continue

    @classmethod
    def save_class_vars(cls, ):

        with open(Bank.bank_file, "w") as f:
            json.dump({"branch_details":Bank.branches_public_details}, f)


    def __init__(self, balance=None, address=None, port=None):

        self._log("Initiating ...")

        Bank.load_class_vars()

        self.branches = []
        self.processes = []
        self.recv_queue = Queue()
        self.id = Bank.next_id
        Bank.next_id += 1
        self.balance = 10000000 if balance is None else balance

        Bank.consts.dir_root.mkdir() if not Bank.consts.dir_root.is_dir() else None
        self.log_database = Bank.consts.dir_root / f"branch_{self.id}.log"
        # self.log_database.mkdir(parents=True) if not self.log_database.is_dir() else None

        self._log(f"The branch {self.id} started working.", in_file=True, file_mode='w')

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
        self._log("Other branches are not started working yet. Waiting for the others ...")

        while True:
            Bank.load_class_vars()

            if len(Bank.branches_public_details) == Bank.n_branches:
                self._log(f"All {Bank.n_branches} branches are open now. Resuming the procedure ...")
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
                    "last_message": None
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
            self._log(message, in_file=True)
            # raise message
            return {"status": False}

        message = {"subject":"transfer", "amount": amount}
        result = self._send_message(receiver["out_conn"], message)

        # check whether status == True or not
        if result["status"]:
            self.balance -= amount

        self._log(f"Branch {self.id}: {amount * Bank.money_unit[0]}{Bank.money_unit[1]:>8} Transferred TO the branch "
                  f"{receiver['id']}. (send_time:{result['send_time']})")
        print("self.balance:", self.balance)

        return result

    @staticmethod
    def _send_message(conn, message):
        """
        Sends a message through the connection conn.
        :param conn: socket connection
        :param message: it can be everything
        :return: a dictionary with these keys: [status: Bool, send_time: datetime object]
        """
        send_time = datetime.now()

        try:
            message = pickle.dumps(message)
            conn.sendall(message)
            status = True
        except:
            status = False

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
        # send = Thread(target=self._do_common_transfer)
        # send.start()

        self._log("do_common")

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

        # for rec in receive:
        #     rec.join()
        #
        # for se in send:
        #     se.join()

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

        max_n_send = 5

        while True:

            if max_n_send == 0:
                print("Reached maximum number of sends")
                return

            sleep(Bank.time_unit)

            if np.random.random() <= 0.3:
                # TODO: define a function to get receiver id: It think I've found a better way of thinking.
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

        '''
        def _do_common_transfer(self):
            """
            Transfers an amount of money in range [1, 1000] to each branch with a probability of 0.3.
            After every transfer, a message will be sent to inspector.
            The Message Structure is like the following example:
                {"subject": "send", "sender_id": 2, "receiver_id": 1, "send_time": datetime.now(), "amount": 500}
            :return: None
            """
            while True:
                # Sleep for a time unit [=0.5 seconds] (between every two transfers).
                sleep(Bank.time_unit)

                for branch in self.branches:
                    if np.random.random() <= 0.3:
                        # TODO: define a function to get receiver id: It think I've found a better way of thinking.
                        receiver = branch
                        amount = np.random.randint(1, 1001)
                        result = self.transfer(amount, receiver)

                        if result["status"]:
                            message = {
                                "subject": "send",
                                "sender_id": self.id,
                                "receiver_id": receiver["id"],
                                "send_time": result["send_time"],
                                "amount": amount,
                            }

                            self._send_message(conn=self.inspector["conn"], message=message)
            # ALMOST DONE
    '''

    def _do_common_receive(self, sender_id):
        """
        Receives an amount of money from a branch with a specific id (in_soc_id).
        It also checks whether a snapshot request has been sent or not. If true, it will toggle on snapshot flag.
        :param sender_id: The id of sender information. The sender itself is a dictionary which its keys are same as the self.branch:
            [id:integer, "port": integer, "address": string,
            "in_sock": socket, "in_conn": input socket connection, "out_conn": output socket connection]
        :return:s
        """

        recv_messages_th = Thread(target=self._recv_messages, args=(sender_id,), name="recv_messages_th")
        recv_messages_th.start()

        sender_index = self._id_to_index(sender_id)

        sender = self.branches[sender_index]

        while True:
            # pickled_data = sender["in_conn"].recv(4096)
            # message = pickle.loads(pickled_data)
            if not self.recv_queue.empty():
                message = self.recv_queue.get()
                # Add an intentional delay to simulate end-to-end connection latency.
                sleep(Bank.time_unit * np.random.randint(1, 11))
            else:
                continue

            print("received: ", message)

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
                # print("received message: ", message)
                self._send_message(conn=self.inspector["conn"], message=message_to_insp)
            elif message["subject"].lower() == "marker":
                self.do_snapshot = sender_index

            elif message["subject"].lower() == "snapshot":
                self.local_snapshots.append(message)

            self.branches[sender_index]["last_message"] = {"recv_time": recv_time,
                                                         "amount": amount,
                                                         "subject": message["subject"]}
            print("IN RECEIVE: self.balance:", self.balance)

    def _recv_messages(self, sender_id):

        sender_index = self._id_to_index(sender_id)
        sender = self.branches[sender_index]
        while True:
            pickled_data = sender["in_conn"].recv(4096)
            message = pickle.loads(pickled_data)
            self.recv_queue.put_nowait(message)


    def snapshot_process(self):

        self._log("snapshot")

        t1 = Thread(target=self._init_snapshot, name="init_snapshot_th")
        t2 = Thread(target=self._check_for_marker, name="check_for_marker_th")

        t1.start()
        t2.start()

        # t1.join()
        # t2.join()

    def _init_snapshot(self):

        while True:
            do_snapshot = input("To get a snapshot-> Type 's' and Enter\n")
            if "s" in do_snapshot:
                self._log("Initiating a snapshot.")
                break

        self.got_marker = True
        local, channels = self._do_snappy_things()
        # print("AFTER DO SNAPPY => local:",local," - channels:",channels)

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

        self._create_global_snapshot()

    def _create_global_snapshot(self):

        # while True:
        message = {
            "subject": "global_snapshot",
            "local_snapshots": [{"id": local["id"],
                                 "balance": local["balance"],
                                 "in_channels": local["on_the_fly"]}
                                for local in self.local_snapshots]
        }

        self._send_message(conn=self.inspector["conn"], message=message)



    def _check_for_marker(self):

        bidx = 0
        while True:
            # print("bidx:", bidx)
            # print('self.branches:', self.branches)
            if self.branches[bidx]["last_message"] is None:
                continue
            if (self.branches[bidx]["last_message"]["subject"] == "marker") or self.do_snapshot >= 0:
                sender_index = self.do_snapshot
                self.do_snapshot = -1
                break
            bidx = (bidx + 1) % (Bank.n_branches - 1)

        self._log(f"Branch {self.branches[sender_index]['id']} has sent a marker.")

        if self.got_marker:
            self._log("Duplicated marker => Ignored.")
            return

        local, channels = self._do_snappy_things(exclude_id=sender_index)
        # print(f"In check4marker, AFTER DO SNAPPY => local:{local} - channels:{channels}")

        try:
            on_the_fly = sum(channels)
        except TypeError:
            on_the_fly = 0

        local_snapshot = {
            "id": self.id,
            "subject": "snapshot",
            "balance": local,
            "on_the_fly": sum(channels)
        }

        self._send_message(conn=self.branches[sender_index]["out_conn"], message=local_snapshot)

    def _do_snappy_things(self, exclude_id=None):

        own_state = self.balance
        message = {"subject": "marker"}

        que = Queue()
        threads = []
        for bindx, branch in enumerate(self.branches):
            self._send_message(branch["out_conn"], message)

            if bindx != exclude_id:
                threads.append(Thread(target=lambda q, arg1: q.put(self._inspect_channel(arg1)),
                                      args=(que, branch["id"],)))
                threads[-1].start()

        # print("tuple(bids): ", tuple(bids))
        # async_result = pool.apply_async(self._inspect_channel, args=(tuple(bids),))  # tuple of args for foo
        # do some other stuff in the main process
        # amounts_in_channels = async_result.get()  # get the return value from your function.
        for th in threads:
            th.join()
        amounts_in_channels = list(que.queue)
        print("amounts_in_channels: ", amounts_in_channels)
        return own_state, amounts_in_channels

        # for th in threads:
        #     th.join()

    def _inspect_channel(self, sender_id):

        sender_index = self._id_to_index(sender_id)

        amount = 0
        last_message = self.branches[sender_index]["last_message"]
        while True:
            if self.branches[sender_index]["last_message"]["recv_time"] > last_message["recv_time"]:
                last_message = self.branches[sender_index]["last_message"]
                if last_message["subject"] == "marker":
                    return amount
                elif last_message["subject"] == "transfer":
                    amount += last_message["amount"]

    def run(self):

        threads = []
        for branch in self.branches:

            # TODO: I may need to call servers before clients. I'm not sure.

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

        # t1.join()
        # t2.join()
        #
        #
        # pool = ThreadPool(2)
        # pool.apply_async(self.do_common)
        # pool.apply_async(self.snapshot_process)

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

    '''
    def _connect_get_set_id(self, i, mode):
        if mode == "server":
            # accept connections from outside
            (self.branches[i]["in_conn"], self.branches[i]["address"]) = self.branches[i]["in_sock"].accept()

            # Receive id of other branches
            data = self.branches[i]["in_conn"].recv(4096)
            branch_id = pickle.loads(data)
            self.branches[i]["id"] = branch_id
        elif mode == "client":
            # connect to other branches
            while True:
                try:
                    self.branches[i]["out_conn"].connect((self.branches[i]["address"], self.branches[i]["port"]))
                    break
                except:
                    continue

            # Send the id of the branch
            self.branches[i]["out_conn"].sendall(pickle.dumps(self.id))
    '''

    def _log(self, message, stdio=True, in_file=False, file_mode="r+"):
        prefix = datetime.now().strftime("%Y%m%d-%H:%M:%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message))

    def _id_to_index(self, id):
        return [i for i, branch in enumerate(self.branches) if branch["id"]==id][0]