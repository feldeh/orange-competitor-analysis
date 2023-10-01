import socket

HOST = "127.0.0.1"
PORT = 4000

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        with conn:
            print(f"connected by {addr}")
            data = conn.recv(1024)
            print(data.decode())
            # header
            conn.send(b'HTTP/1.1 200 OK\n')
            conn.send(b'Content-Type: text/html\n')
            conn.send(b'\n')
            # body
            conn.send(b'<html><body><pre>')
            conn.send(data)
            conn.send(b'</pre></body></html>')
            conn.close()

