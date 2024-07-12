import hashlib
import pathlib
from collections import namedtuple
import socket
from concurrent.futures import ThreadPoolExecutor
import time


path = "./sample"

ChunkElement = namedtuple("ChunkElement", ["chunk", "checksum", "next_checksum"])


def recursive_list_files(path):
    files = []
    for obj in pathlib.Path(path).glob("*"):
        if obj.is_file():
            files.append(obj)
        elif obj.is_dir():
            files.extend(recursive_list_files(obj))
    return files

def file_checksum(file):
    with open(file, "rb") as f:
        md5 = hashlib.md5()
        while True:
            chunk = f.read(1024)
            if not chunk:
                break
            md5.update(chunk)
        return md5.hexdigest()

def chunk_file(file, chunk_size=1024):
    def _chunk_file(file, chunk_size):
        with open(file, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk, hashlib.md5(chunk).hexdigest()

    it = _chunk_file(file, chunk_size)
    elem = next(it)
    while elem:
        try:
            nx_elem = next(it)
        except StopIteration:
            nx_elem = None
        yield ChunkElement(*elem, nx_elem[1] if nx_elem else None)
        elem = nx_elem


files = recursive_list_files(path)
print(files[2].name)
for chk in chunk_file(files[2], 4):
    print(chk)


checksums = {str(file): file_checksum(file) for file in files}
print(checksums)

"""
server commands format:
    - list_files -> "LST_FILES\r\n"
    - get_file_checksum <file_id> -> "GET_FILE_CHECKSUM <file_id>\r\n"
    - close connection -> "CLOSE\r\n"
    - sleep (for testing purposes) -> "SLEEP <seconds>\r\n"
"""
def server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("localhost", 12345))
    s.listen(10)

    thread_pool = ThreadPoolExecutor(max_workers=10)

    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")

        def _handle_conn(conn):
            while True:
                data = conn.recv(1024)
                if not data:
                    break

                cmd, *args = data.decode().strip().split(" ")
                if cmd == "LST_FILES":
                    for file_id, cks in checksums.items():
                        conn.sendall(f"{file_id} {cks}\r\n".encode())
                elif cmd == "GET_FILE_CHECKSUM":
                    file_id = args[0]
                    conn.sendall(f"{file_id} {checksums[file_id]}\r\n".encode())
                elif cmd == "CLOSE":
                    conn.sendall("Goodbye!\r\n".encode())
                    conn.close()
                    break
                elif cmd == "SLEEP":
                    conn.sendall(f"Sleeping for {args[0]} seconds\r\n".encode())
                    time.sleep(int(args[0]))
                    conn.sendall("Awake!\r\n".encode())
                else:
                    conn.sendall(f"Unknown command {cmd}\r\n".encode())

        thread_pool.submit(_handle_conn, conn)

server()