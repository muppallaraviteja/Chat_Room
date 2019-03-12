import multiprocessing
import pprint
import threading
from concurrent.futures import thread
from threading import Thread


import redis


from mongoConnect import connect_mongo
dir(redis)

r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
db= connect_mongo()

subscribing = False
board_flag = False
board_name = ""


def valid_command(command):
    cmd_list = ["select", "read", "write", "stop", "quit", "listen"]
    if command in cmd_list:
        return True
    return False


def listen_command(b_name):
    try:
        p = r.pubsub()
        re = p.subscribe(b_name)
        print(re)
        if subscribing:
            for item in p.listen():
                print(item)
    except KeyboardInterrupt:
        return


while True:
    try:
        cmd = input('Enter your command: ')
        print(cmd)
        cmd_parts = cmd.split(" ")
        print(cmd_parts)
        message = ""
        if cmd_parts[0].lower() == "select":
            if len(cmd_parts) <= 1:
                message = 'Wrong Format of :' + cmd_parts[0]
            else:
                board_flag = True
                board_name = ''.join(cmd_parts[1:])
        elif cmd_parts[0].lower() == "write" and board_flag:
            if len(cmd_parts) <= 1:
                message = 'Wrong Format of :' + cmd_parts[0]
            else:
                collection = db[board_name]
                data = {"message": ' '.join(cmd_parts[1:])}
                collection.insert_one(data)
                to_pub = ' '.join(cmd_parts[1:])
                res = r.publish(board_name, to_pub)
                print(res)
        elif cmd_parts[0].lower() == "read" and board_flag:
            if len(cmd_parts) > 1:
                message = 'Wrong Format of :' + cmd_parts[0]
            else:
                collection = db[board_name]
                all_messages = collection.find()
                for message in all_messages:
                    pprint.pprint(message)
        elif cmd_parts[0].lower() == "listen" and board_flag:
            if len(cmd_parts) > 1:
                message = 'Wrong Format of :'+cmd_parts[0]
            else:
                subscribing = True
                t = threading.Thread(target=listen_command, args=(board_name,))
                t.setDaemon(True)
                t.start()

        elif cmd_parts[0].lower() == "stop" and board_flag:
                if not subscribing:
                    message = "Please listen to the channel before you use stop"
                else:
                    subscribing = False
        elif cmd_parts[0] == "quit":
            break
        elif valid_command(cmd_parts[0]):
            message = "Please select Channel before using command :" + cmd_parts[0]
        elif not valid_command(cmd_parts[0]):
            message = "Invalid Command"

        if len(message) >= 0:
            print(message)
    except KeyboardInterrupt:
        subscribing = False
