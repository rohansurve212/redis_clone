import socket
from threading import Thread
from pyredis.protocol import parse_frame, Array, Error, SimpleString, BulkString, Integer

HOST = "0.0.0.0"
PORT = 7

# Setup server to listen for connections
def start_server():
    """_summary_
        The start_server function sets up a basic TCP server using Python’s socket module and 
        uses threading to handle multiple client connections concurrently
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"Server listening on {HOST}:{PORT}")
        
        while True:
            client_socket, client_address = server_socket.accept()
            print(f"New connection from {client_address}")
            Thread(target=handle_client, args=(client_socket,)).start()

# Handle client connections
def handle_client(client_socket):
    # Keep handling the client as long as the socket is alive
    while client_socket:
        # Initialize buffer
        buffer = b""
        try:
            while True:
                try:
                    # Receive data from client
                    data = client_socket.recv(4096)
                    if not data:
                        print("Client disconnected")
                        client_socket.close()
                        return # Exit the function
                    
                    print(f"Raw data received: {data}")
                    # Append newly received data to the buffer
                    buffer += data
                    while buffer:
                        frame, consumed = parse_frame(buffer)
                        if frame is None:
                            break # Wait for more data
                        
                        # Remove processed data
                        buffer = buffer[consumed:]

                        # Handle the command
                        response = process_command(frame)
                        print(f"Sending response: {response}")
                        client_socket.sendall(response)
                except Exception as e:
                    print(f"Error: {e}")
                    break
        except Exception as e:
            print(f"Error: {e}")
            client_socket.close()
            client_socket = None
            break

# Handle the command
def process_command(frame):
    if isinstance(frame, Array):
        if not frame.elements:
            return Error("Empty command").encode()
        
        command = frame.elements[0].data.upper()
        if command == "COMMAND":
            # Stub response for COMMAND
            return BulkString("OK").encode()
        
        elif command == "INFO":
            if len(frame.elements) > 1 and frame.elements[1].data.upper() == "SERVER":
                return BulkString("# Server\nredis_version:0.1.0\n").encode()
            return BulkString("").encode()
        
        elif command == "ECHO":
            # Handle ECHO command
            if len(frame.elements) < 2:
                return Error("ECHO requires an argument").encode()
            concatenated = " ".join(element.data for element in frame.elements[1:])
            return BulkString(concatenated).encode()
        
        elif command == "PING":
            # Handle PING command
            if len(frame.elements) > 1:
                return SimpleString(frame.elements[1].data).encode()
            return SimpleString("PONG").encode()
        
        else:
            return Error("Invalid command").encode()

    return Error("Invalid frame type").encode()

if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("Server shutting down...")