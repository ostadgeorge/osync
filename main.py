import hashlib
import pathlib
from collections import namedtuple
import socket
from concurrent.futures import ThreadPoolExecutor
import time
import os
import argparse


ChunkElement = namedtuple(
    "ChunkElement", ["chunk", "checksum", "next_checksum", "chunk_idx"]
)

SERVER_IP = "127.0.0.1"
SERVER_PORT = 8892
CHUNK_SIZE = 1024


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

    idx = 0
    it = _chunk_file(file, chunk_size)
    elem = next(it)
    while elem:
        try:
            nx_elem = next(it)
        except StopIteration:
            nx_elem = None
        yield ChunkElement(*elem, nx_elem[1] if nx_elem else None, idx)
        elem = nx_elem
        idx += 1


def ith_chunk(file, i, chunk_size=1024):
    with open(file, "rb") as f:
        f.seek(i * chunk_size)
        data = f.read(chunk_size)
        nx_data = f.read(chunk_size)
        return ChunkElement(
            data,
            hashlib.md5(data).hexdigest(),
            hashlib.md5(nx_data).hexdigest() if nx_data else None,
            i,
        )


def number_of_chunks(file, chunk_size=1024):
    file_size = os.stat(file).st_size
    return file_size // chunk_size + (1 if file_size % chunk_size else 0)


def serve(ip, port, path, chunk_size):
    """
    server commands format:
        - list_files -> "LST_FILES\r\n"
        - get_file_checksum <file_id> -> "FILE_CHECKSUM <file_id>\r\n"
        - number_of_chunks <file_id> <chunck_size> -> "NUM_CHUNKS <file_id> <chunk_size>\r\n"
        - get_chunk <file_id> <chunk_idx> <chunk_size> -> "CHUNK <file_id> <chunk_idx> <chunk_size>\r\n"
        - close connection -> "CLOSE\r\n"
        - sleep (for testing purposes) -> "SLEEP <seconds>\r\n"
    """

    files = recursive_list_files(path)
    p = str(path).split("/")[-1]
    checksums = {str(file).split(p)[-1]: file_checksum(file) for file in files}

    print(checksums)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((ip, port))
    s.listen(10)

    thread_pool = ThreadPoolExecutor(max_workers=10)

    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")

        def _handle_conn(conn, chunk_size):
            while True:
                data = conn.recv(chunk_size)
                if not data:
                    break

                cmd, *args = data.decode().strip().split(" ")
                if cmd == "LST_FILES":
                    for file_id, cks in checksums.items():
                        conn.sendall(f"{file_id} {cks}\r\n".encode())
                    conn.sendall("END\r\n".encode())
                elif cmd == "FILE_CHECKSUM":
                    file_id = args[0]
                    conn.sendall(f"{checksums[file_id]}\r\n".encode())
                    conn.sendall("END\r\n".encode())
                elif cmd == "NUM_CHUNKS":
                    file_id, dchunk_size = args
                    file_id = f"{path}/{file_id}"
                    conn.sendall(
                        f"{number_of_chunks(file_id, int(dchunk_size))}\r\n".encode()
                    )
                    conn.sendall("END\r\n".encode())
                elif cmd == "CHUNK":
                    file_id, chunk_idx, dchunk_size = args
                    file_id = f"{path}/{file_id}"
                    chunk = ith_chunk(file_id, int(chunk_idx), int(dchunk_size))
                    conn.sendall(f"IDX>{chunk.chunk_idx}\r\n".encode())
                    conn.sendall(f"CHK>{chunk.checksum}\r\n".encode())
                    conn.sendall(f"NX_CHK>{chunk.next_checksum}\r\n".encode())
                    conn.sendall("DATA>".encode())
                    conn.sendall(chunk.chunk + "\r\n".encode())
                    conn.sendall("END\r\n".encode())
                elif cmd == "CLOSE":
                    conn.sendall("Goodbye!\r\n".encode())
                    conn.close()
                    break
                elif cmd == "SLEEP":
                    conn.sendall(f"Sleeping for {args[0]} seconds\r\n".encode())
                    time.sleep(int(args[0]))
                    conn.sendall("Awake!\r\n".encode())
                elif cmd == "":
                    pass
                else:
                    conn.sendall(f"Unknown command {cmd}\r\n".encode())

        # thread_pool.submit(_handle_conn, conn)
        try:
            _handle_conn(conn, chunk_size)
        except Exception as e:
            print(e)
            conn.close()


def client(ip, port, path, chunk_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))

    server_files = {}
    local_files = {}

    def _sync_server_files():
        s.sendall("LST_FILES\r\n".encode())
        server_files.clear()

        data = "".encode()
        end = False
        while not end:
            data += s.recv(chunk_size)
            if not data:
                break
            lines = data.decode().split("\r\n")
            lines = [line for line in lines if line]
            for line in lines:
                if line == "END":
                    end = True

                try:
                    file_id, cks = line.split(" ")
                    server_files[file_id] = cks
                except Exception as e:
                    data = lines[-1].encode()
                    break

    def _sync_local_files():
        local_files.clear()
        p = str(path).split("/")[-1]
        for file in recursive_list_files(path):
            local_files[str(file).split(p)[-1]] = file_checksum(file)

    def _remove_files():
        def _files_to_remove():
            for lf, lv in local_files.items():
                remove = True
                for sf, sv in server_files.items():
                    if lf == sf and lv == sv:
                        remove = False
                        break
                if remove:
                    yield lf

        for f in _files_to_remove():
            f = f"{path}/{f}".replace("//", "/").replace("//", "/")
            os.remove(f)

    def _download_files():
        def _files_to_download():
            for sf, sv in server_files.items():
                download = True
                for lf, lv in local_files.items():
                    if sf == lf and sv == lv:
                        download = False
                        break
                if download:
                    yield sf

        def _num_chunks(file):
            s.sendall(f"NUM_CHUNKS {file} {chunk_size}\r\n".encode())

            data = "".encode()
            num_chunks = None
            while num_chunks is None:
                data += s.recv(chunk_size)
                if not data:
                    break
                if "END\r\n" not in data.decode():
                    continue
                lines = data.decode().split("\r\n")
                lines = [line for line in lines if line]
                for line in lines:
                    if line == "END":
                        break

                    num_chunks = line
                    break

            return int(num_chunks)

        def _download_file(file):
            num_chunks = _num_chunks(file)
            print(f"Downloading {file} with {num_chunks} chunks")

            addr = f"{path}/{file}"
            os.makedirs(os.path.dirname(addr), exist_ok=True)

            with open(f"{path}/{file}", "wb") as f:
                for i in range(int(num_chunks)):
                    s.sendall(f"CHUNK {file} {i} {chunk_size}\r\n".encode())
                    data = "".encode()
                    end = False
                    while not end:
                        data += s.recv(chunk_size)
                        if not data:
                            break
                        if not "END\r\n" in data.decode():
                            continue
                        lines = data.decode().split("\r\n")
                        lines = [line for line in lines if line]
                        for line in lines:
                            if line == "END":
                                end = True
                            elif line.startswith("DATA>"):
                                f.write(line[5:].encode())

        for file in _files_to_download():
            _download_file(file)

    def _close_connection():
        s.sendall("CLOSE\r\n".encode())
        s.close()

    _sync_server_files()
    _sync_local_files()
    _remove_files()
    _download_files()
    _close_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--client", action="store_true")
    parser.add_argument("--ip", type=str, default=SERVER_IP)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--chunk_size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--path", type=str)
    args = parser.parse_args()

    if args.serve:
        serve(args.ip, args.port, args.path, args.chunk_size)
    elif args.client:
        client(args.ip, args.port, args.path, args.chunk_size)
    else:
        parser.print_help()
