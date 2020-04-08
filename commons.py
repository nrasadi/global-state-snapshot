import numpy as np
from datetime import datetime
from time import sleep
from multiprocessing import Pool
import matplotlib.pyplot as plt
import socket
from pathlib import Path
import json
import pickle
import threading
from threading import Thread


class Constants:
    dir_root = Path("logs/")


class Bank:

    # Class Variables
    next_id = 0
    consts = Constants()
    money_unit = 1000, "Toomaan"
    time_unit = 0.5 # Seconds

    def __init__(self, balance=None, address=None, port=None, n_branches=4):

        self.log_database = Bank.consts.dir_root / f"branch_{self.id}.log"

        self._log("Initiating ...")

        self.n_branches = n_branches
        self.branches = []
        self.processes = []
        self.id = Bank.next_id
        Bank.next_id += 1
        self.balance = 10000000 if balance is None else balance

        self._log(f"The branch {self.id} started working.", in_file=True, file_mode='w')

        self.snapshot_flag = False

        self.address = "localhost" if address is None else address

        # out ports: 99[self.id][others]
        #  in ports: 99[others][self.id]
        self.port_base = 9900 + self.id * 10 if port is None else port

        # Create INET, Sreaming sockets
        self.out_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind and Listen
        self.in_soc = []
        self.out_soc = []
        for i in range(self.n_branches - 1):



        self.inspector = {"port": 10101, "address": "localhost", "conn": None}
        self._init_inspector()
        # Bank.branches_details.append({"id": self.id, "address": self.address, "port_base": self.port_base})

    def _init_other_branches(self):

        for i in range(self.n_branches):
            if i != self.id:
                # TODO: modify address

                port =  9900 + self.id + i * 10
                self.branches.append({
                    "id": i, # It will change after the first connection
                    "port": port,
                    "address": "localhost",
                    "in_sock": socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    "in_conn": None,
                    "out_conn": socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                })
                self.branches[-1]["in_sock"].bind((self.address, port))
                self.branches[-1]["in_sock"].listen(1)

    def _init_inspector(self):
        insp_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        insp_soc.connect((self.inspector["address"], self.inspector["port"]))
        self.inspector["conn"] = insp_soc


    def transfer(self, amount, receiver):
        """

        :param amount: amount
        :param receiver: {"id": branch_id, "conn": out tcp connection}
        :return:
        """
        # TODO: Write something like the below:
        if amount > self.balance:
            message =f"The amount of money needed to transfer is more than asset of the branch #{self.id}."
            self._log(message, in_file=True)
            raise message
            
        result = self._send_message(receiver["conn"], {"transfer": amount, "marker": False})

        # check whether status == True or not
        if result["status"]:
            self.balance -= amount


        self._log(f"Branch {self.id}: {amount * Bank.money_unit[0]}{Bank.money_unit[1]} Transferred TO the branch "
                  f"{receiver['id']}.")

        return result

    def _send_message(self, conn, message):

        send_time = datetime.now()

        # TODO: Not implemented yet
        try:
            message = pickle.dumps(message)
            conn.sendall(message)
            status = True
        except:
            status = False

        return {
            "status": status, # True (success) or False (fail)
            "send_time": send_time,
            "receive_time": None # TODO: To be added
        }
        # return successful/failed (1 or 0)


    def do_common(self):

        send = Thread(target=self._do_common_transfer)
        send.start()

        receive = []
        for i in range(self.n_branches - 1):
            receive.append(Thread(target=self._do_common_receive, args=(i, )))
            receive[i].start()

    def _do_common_transfer(self):

        while True:
            sleep(0.5)

            for branch in self.branches:
                if np.random.random() <= 0.3:

                    # TODO: define a function to get receiver id
                    receiver =
                    amount = np.random.random(1, 1001)
                    result = self.transfer(amount, receiver)

                    if result["status"]:
                        message = {
                            "common":{
                                "sender_id": self.id,
                                "send_time": result["send_time"],
                                "amount": amount,
                                "receiver_id": receiver,
                                "receive_time": result["receive_time"]
                            },
                            "snapshot": None
                        }
                        # TODO: correct receiver (inspector details)
                        self._send_message(receiver=Bank.inspector["conn"], message=message)
        # ALMOST DONE

    def _do_common_receive(self, in_soc_id):

        while True:
            pickled_data = self.in_soc[in_soc_id].recv(4096)
            self.branches

            data = pickle.loads(pickled_data)

            self.snapshot_flag = True if data["marker"] else False

    def do_snapshot(self):


        t3 = Thread(target=self.init_snapshot)
        t4 = Thread(target=self.do_snapshot)

        """
        do = True
        while do:
            inp = input("Initiate a Snapshot? [Enter]")
            print("Initiating Snapshot...")
            self.snapshot()
            # TODO : snapshot precedure
            pass
        """

    def run(self):

        threads = []
        for i in range(self.n_branches):
            threads.append(Thread(target=self._connect_get_set_id, args=(i, "server",)))
            threads.start()

            threads.append(Thread(target=self._connect_get_set_id, args=(i, "client",)))
            threads.start()

        for th in threads:
            th.join()

        pool = Pool(2)
        pool.apply_async(self.do_common())
        pool.apply_async(self.do_snapshot())

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




    def _log(self, message, stdio=True, in_file=False, file_mode="r+"):
        prefix = datetime.now().strftime("%Y-%m-%d-%H-%M-%S ")

        if stdio:
            print(prefix + str(message))

        if in_file:
            with open(self.log_database, mode=file_mode) as f:
                f.write(prefix + str(message))


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


    def snapshot(self):

        # Record the Own Process State
        own_balance = self.balance

        # Send Marker Messages
        for receiver in All_Receivers:
            self._send_message(receiver, {"marker": True, "transfer": 0})

        # Record Incoming Messages
        # TODO: Listen to all incoming channels

    def get_messages(self, sender_id):

        # Listen to all channels

        while True:
            # now do something with the clientsocket
            # in this case, we'll pretend this is a threaded server
            ct = client_thread(clientsocket)
            ct.run()

        sleep(np.random.random(1, 11))



        pass
