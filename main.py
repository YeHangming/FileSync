from socket import *
import json
import multiprocessing as mp
import os
import time
import struct
import threading
import argparse


def _argparse():
    parser = argparse.ArgumentParser(description="This is description!")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='The ip addresses of three hosts')
    parser.add_argument('--encryption', action='store', nargs='?', const=0, type=str, default='no',
                        dest='encryption', help='whether to encrypt')
    return parser.parse_args()


def listener(port, log, encryption):
    r_socket = socket(AF_INET, SOCK_STREAM)
    r_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    r_socket.bind(('', port))
    while True:
        r_socket.listen(50)
        con, add = r_socket.accept()
        print("RM | ", add, " connected")
        thread = threading.Thread(target=receiveManager, args=(con, add, encryption))
        thread.start()


def heart(port):
    r_socket = socket(AF_INET, SOCK_STREAM)
    r_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    r_socket.bind(('', port))
    while True:
        r_socket.listen(50)
        r_socket.accept()


def receiveManager(con, add, encryption):
    log = readLog("tmp/log.json")
    log_bin = json.dumps(log).encode('utf-8')
    log_size = len(log_bin)
    head = struct.pack('!I', log_size)
    print("RM | log size:", log_size)
    con.sendall(head)
    con.send(log_bin)
    print("RM | log send to ", add)
    while True:
        try:
            cmd = struct.unpack('!i', con.recv(4))[0]
            print("received")
        except:
            continue
        port_num = get_free_port()
        con.send(struct.pack('!I', port_num))
        if cmd == 0:
            print("RM | file transfer request received from", add, " receive port:", port_num)
            t = threading.Thread(target=receiver, args=(port_num, log, encryption,))
            t.start()
        if cmd == 1:
            print("RM | partial update request received from", add, " receive port:", port_num)
            t = threading.Thread(target=partialUpdate, args=(port_num, log,))
            t.start()


def sendManager(ip, port, encryption, log):
    while True:
        s_socket = socket(AF_INET, SOCK_STREAM)
        try:
            s_socket.connect((ip, port))
            temp = s_socket.recv(4)
        except Exception:
            continue
        print('SM | connected to receiver:', ip)
        log_size = struct.unpack('!I', temp)[0]
        log_r = json.loads(s_socket.recv(log_size).decode('utf-8'))
        print("SM | log received", ip, "  Content:", log_r)
        diff = compareDiff(log, log_r)
        print("SM | difference find: ", diff)
        alreadysend = {}
        flag = 0
        for f in diff:
            s_socket.send(struct.pack('!I', 0))
            print("SM | send file transfer request to ", ip)
            port_num = struct.unpack('!i', s_socket.recv(4))[0]
            print("SM | request approved, trying to pass new port ", port_num, " to subthread")
            t = threading.Thread(target=sender, args=(ip, port_num, f, encryption,))
            t.run()
        while True:
            cmd, file_info = detectChanges("share")
            if cmd == 0 and alreadysend.get(file_info['name']) is None:
                s_socket.send(struct.pack('!I', 0))
                print("SM | send file transfer request to ", ip)
                port_num = struct.unpack('!i', s_socket.recv(4))[0]
                print("SM | trying to pass new port ", port_num, " to subthread")
                t = threading.Thread(target=sender, args=(ip, port_num, file_info, encryption,))
                t.run()
            if cmd == 1 and flag == 0:
                flag += 1
                s_socket.send(struct.pack('!I', 1))
                print("SM | send partial update request to ", ip)
                port_num = struct.unpack('!i', s_socket.recv(4))[0]
                print("SM | trying to pass new port ", port_num, " to subthread")
                t = threading.Thread(target=partialSend, args=(ip, port_num, file_info,))
                t.run()


def receiver(port, log, encryption):
    fmt = '256sI'
    recv_buffer = 20480
    listenSock = socket(AF_INET, SOCK_STREAM)
    listenSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    listenSock.bind(('', port))
    print("R | port ", port, " bind")
    while True:
        listenSock.listen(50)
        conn, addr = listenSock.accept()
        starttime = time.time()
        print("R | host connected:", addr)
        headsize = struct.calcsize(fmt)
        head = conn.recv(headsize)
        path = struct.unpack(fmt, head)[0].decode().rstrip('\0')
        folder = os.path.split(path)[0]
        os.makedirs(folder, exist_ok=True)
        filesize = struct.unpack(fmt, head)[1]
        print("R | ready to receive file: " + path + "\nfilesize:" + str(filesize))
        received_size = 0
        log[path] = dict(name=path, mtime1=0, state=1, size=0)
        writeLog("tmp/log.json", log)
        fd = open(path, 'wb')
        while True:
            data = conn.recv(recv_buffer)
            received_size = received_size + len(data)
            fd.write(data)
            if received_size == filesize:
                break
        fd.close()
        log[path] = dict(name=path, mtime1=os.path.getmtime(path), state=0, size=os.path.getsize(path))
        writeLog("tmp/log.json", log)
        print("R | file received: " + path + "\nfilesize:" + str(filesize))
        endtime = time.time()
        speed = filesize/1024/1024/(endtime - starttime)*8
        print("time for receive ", path, " is ", endtime - starttime, "speed is", speed, "Mbps")
        break
    listenSock.close()


def sender(ip, port, file, encryption):
    try:
        fmt = '256sI'
        send_buffer = 20480
        path = file['name']
        filesize = file['size']
        sock = socket(AF_INET, SOCK_STREAM)
        print("S | Try to connect receiver", ip, port)
        while True:
            try:
                sock.connect((ip, port))
            except Exception as ex:
                print(ex)
                continue
            print("S | connected to receiver", ip, port)
            head = struct.pack(fmt, path.encode('utf-8'), filesize)
            sock.sendall(head)
            print("S | Trying to send file:" + path + "  filesize:" + str(filesize))
            restSize = filesize
            fd = open(path, 'rb')
            while restSize >= send_buffer:
                data = fd.read(send_buffer)
                sock.sendall(data)
                restSize = restSize - send_buffer
            data = fd.read(restSize)
            sock.sendall(data)
            print("successfully sent file ", path, " to ip: ", ip)
            fd.close()
            log = readLog("tmp/log.json")
            log[file['name']] = dict(name=path, mtime1=os.path.getmtime(path), state=0, size=os.path.getsize(path))
            writeLog("tmp/log.json", log)
            print("log writed after sending to ", ip)
            sock.close()
            break
    except Exception:
        pass


def partialSend(ip, port, file):
    fmt = '256sI'
    send_buffer = 2048
    path = file['name']
    filesize = int(file['size'] / 1000) + 5
    sock = socket(AF_INET, SOCK_STREAM)
    while True:
        try:
            sock.connect((ip, port))
        except Exception:
            continue
        print("filename:" + path + "\nfilesize:" + str(filesize))
        head = struct.pack(fmt, path.encode('utf-8'), filesize)
        sock.sendall(head)
        restSize = filesize
        fd = open(path, 'rb')
        while restSize >= send_buffer:
            data = fd.read(send_buffer)
            sock.sendall(data)
            restSize = restSize - send_buffer
        data = fd.read(restSize)
        sock.sendall(data)
        fd.close()
        print("successfully sent 1% file ", path, " to ip: ", ip)
        sock.close()
        break


def partialUpdate(port, log):
    fmt = '256si'
    recv_buffer = 2048
    data_front = b''
    listenSock = socket(AF_INET, SOCK_STREAM)
    listenSock.bind(('', port))
    while True:
        listenSock.listen(5)
        conn, addr = listenSock.accept()
        headsize = struct.calcsize(fmt)
        head = conn.recv(headsize)
        path = struct.unpack(fmt, head)[0].decode().rstrip('\0')
        folder = os.path.split(path)[0]
        os.makedirs(folder, exist_ok=True)
        filesize = struct.unpack(fmt, head)[1]
        print("filename:" + path + "\nfilesize:" + str(filesize))
        received_size = 0
        log[path] = dict(name=path, mtime1=float('inf'), state=1, size=log[path]['size'])
        writeLog("tmp/log.json", log)
        fd = open(path, 'rb')
        fd.seek(filesize)
        data_behind = fd.read()
        fd.close()
        while True:
            data = conn.recv(recv_buffer)
            received_size = received_size + len(data)
            data_front += data
            if received_size == filesize:
                break
        fw = open(path, 'wb')
        fw.write(data_front)
        fw.write(data_behind)
        fw.close()
        log[path] = dict(name=path, mtime1=os.path.getmtime(path), state=0, size=log[path]['size'])
        writeLog("tmp/log.json", log)
        break
    listenSock.close()


def get_free_port():
    sock = socket()
    sock.bind(('', 0))
    ip, port = sock.getsockname()
    sock.close()
    return port


def traverse(dir_path):
    file_list = []
    file_folder_list = os.listdir(dir_path)
    for file_folder_name in file_folder_list:
        if os.path.isfile(os.path.join(dir_path, file_folder_name)):
            file_list.append(os.path.join(dir_path, file_folder_name))
        else:
            file_list.extend(traverse(os.path.join(dir_path, file_folder_name)))
    return file_list


def createLog(dir_path):
    file_list = traverse(dir_path)
    tmp = {}
    for f in file_list:
        info = dict(name=f, mtime1=os.path.getmtime(f), state=0, size=os.path.getsize(f))
        tmp[f] = info
    return tmp


def writeLog(path, log):
    with open(path, 'w') as fw:
        json.dump(log, fw)


def readLog(path):
    file = open(path, 'r')
    log = json.loads(file.read())
    file.close()
    return log


def logInit(dir_path, log_path):
    if os.path.exists(log_path):
        log = readLog(log_path)
    else:
        log = createLog(dir_path)
        os.makedirs("tmp", exist_ok=True)
        writeLog(log_path, log)
    return log


def detectChanges(path):
    while True:
        time.sleep(0.05)
        log = readLog("tmp/log.json")
        info = createLog(path)
        for i in info:
            if log.get(i) is None:
                log[i] = info[i]
                log[i]['state'] = 0
                return 0, info[i]
            elif info[i]['mtime1'] > log[i]['mtime1'] and log[i]['state'] == 0:
                log[i] = info[i]
                return 1, log[i]


def compareDiff(log_s, log_r):
    diff = []
    log_s  = readLog("tmp/log.json")
    for i in log_s:
        if log_r.get(i) is None:
            diff.append(log_s[i])
        elif log_r[i]['size'] < log_s[i]['size']:
            diff.append(log_s[i])
    return diff

def reconnect(ip,log):
    flag = 1
    time.sleep(5)
    while True:
        try:
            s = socket(AF_INET, SOCK_STREAM)
            s.connect((ip, 30000))
        except Exception:
            flag = 0
            time.sleep(2)
        else:
            if flag == 0:
                print("reconnected")
                sm = mp.Process(target=sendManager, args=(ip, 20000, encryption, log,))
                sm.start()
                break

if __name__ == '__main__':
    log_path = "tmp/log.json"
    share_path = "share"
    parser = _argparse()
    all_ip = parser.ip.split(',')
    encryption = parser.encryption
    print("parameter analysis done, ip: ", all_ip[0], "  ", all_ip[1], "  encryption: ", encryption)
    if encryption == 'yes':
        encryption = True
    else:
        encryption = False
    print(encryption)
    os.makedirs("share", exist_ok=True)
    logfile = logInit("share", "tmp/log.json")
    print("log initialized")
    rm = mp.Process(target=listener, args=(20000, logfile, encryption,))
    rm.start()
    sm1 = mp.Process(target=sendManager, args=(all_ip[0], 20000, encryption, logfile,))
    sm1.start()
    sm2 = mp.Process(target=sendManager, args=(all_ip[1], 20000, encryption, logfile,))
    sm2.start()
    thread0 = threading.Thread(target=heart, args=(30000,))
    thread0.start()
    thread1 = threading.Thread(target=reconnect, args=(all_ip[0], logfile,))
    thread1.start()
    thread2 = threading.Thread(target=reconnect, args=(all_ip[1], logfile,))
    thread2.start()