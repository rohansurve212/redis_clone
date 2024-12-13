import asyncio, time, random
from functools import partial
from pyredis.protocol import parse_frame, Array, Error, SimpleString, BulkString, Integer

HOST = "0.0.0.0"
PORT = 7
CONCURRENCY_METHOD = "ASYNCIO"

# Setup server to listen for connections
async def start_server_using_asyncio(STORE, STORE_LOCK):
    """
        Start the asyncio server
    """
    server = await asyncio.start_server(partial(handle_client_using_asyncio, STORE, STORE_LOCK), HOST, PORT)
    addr = server.sockets[0].getsockname()
    print(f"Server listening on {addr}")
    
    async with server:
        await server.serve_forever()

# Handle client connections
async def handle_client_using_asyncio(STORE, STORE_LOCK, reader, writer):
    """Handle a single client connection."""
    addr = writer.get_extra_info('peername')
    buffer = b""

    try:
        while True:
            # Read data from the client
            data = await reader.read(4096)
            if not data:
                print(f"Client disconnected")
                break
            
            print(f"Raw data received from {addr}: {data}")
            buffer += data

            # Process complete frames
            while buffer:
                frame, consumed = parse_frame(buffer)
                print(f"Frame Elements - {frame.elements}")
                if frame is None:
                    break  # Wait for more data
                
                buffer = buffer[consumed:]  # Remove processed data

                # Handle the command
                response = await async_process_command(frame, STORE, STORE_LOCK)
                print(f"Sending response: {response}")
                writer.write(response)
                await writer.drain()  # Ensure the response is sent
    except Exception as e:
        # print(f"Error handling client: {e}")
        pass
    finally:
        # print(f"Closing connection")
        writer.close()
        await writer.wait_closed()

# Handle the command
async def async_process_command(frame, STORE, STORE_LOCK):
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
        
        elif command == "SET":
            # Handle SET command with expiry
            if len(frame.elements) < 3:
                return Error("SET requires a key and a value").encode()
            
            key = frame.elements[1].data
            value = frame.elements[2].data
            expiry_time = None
            
            # Check for optional expiry argument (EX or PX)
            if len(frame.elements) > 4:
                option = frame.elements[3].data.upper()
                if option == "EX": # Expiry in seconds
                    expiry_time = time.time() + int(frame.elements[4].data)
                elif option == "PX": # Expiry in milliseconds
                    expiry_time = time.time() + int(frame.elements[4].data) / 1000

            async with STORE_LOCK: # Acquire asyncio Lock
                STORE[key] = (value, expiry_time) # Store the key-value pair, overwrite value if key already exists
            
            return SimpleString("OK").encode()
        
        elif command == "GET":
            # Handle GET command with expiry check
            if len(frame.elements) < 2:
                return Error("GET requires a key").encode()
            
            key = frame.elements[1].data
            async with STORE_LOCK: # Acquire asyncio Lock
                entry = STORE.get(key) # Retrieve the value for the key

            if entry is None:
                return BulkString(None).encode() # RESP null bulk string for missing keys

            value, expiry_time = entry
            # Check if the key has expired
            if expiry_time is not None and time.time() > expiry_time:
                async with STORE_LOCK:
                    del STORE[key] # Remove expired key
                return BulkString(None).encode()
            
            return BulkString(value).encode()
        
        elif command == "LPUSH":
            # Handle LPUSH command
            if len(frame.elements) < 3:
                return Error("LPUSH requires a key and a value").encode()

            key = frame.elements[1].data
            values = [element.data for element in frame.elements[2:]]
            
            async with STORE_LOCK: # Acquire asyncio Lock
                if key in STORE:
                    if isinstance(STORE[key], list):
                        STORE[key] = values + STORE[key] # Prepend values to the list
                    else:
                        return Error("WRONGTYPE: Operation against a key holding the wrong kind of value").encode()
                else:
                    STORE[key] = values # Create a new list
                
                # Get the length of the list
                len_entry = str(len(STORE[key]))
            
            return SimpleString(f"(integer) {len_entry}").encode()

        elif command == "RPUSH":
            # Handle RPUSH command
            if len(frame.elements) < 3:
                return Error("RPUSH requires a key and a value").encode()

            key = frame.elements[1].data
            values = [element.data for element in frame.elements[2:]]
            
            async with STORE_LOCK: # Acquire asyncio Lock
                if key in STORE:
                    if isinstance(STORE[key], list):
                        STORE[key] = STORE[key] + values # Append values to the list
                    else:
                        return Error("WRONGTYPE: Operation against a key holding the wrong kind of value").encode()
                else:
                    STORE[key] = values # Create a new list
                
                # Get the length of the list
                len_entry = str(len(STORE[key]))
            
            return SimpleString(f"(integer) {len_entry}").encode()
        
        elif command == "LRANGE":
            # Handle LRANGE command
            if len(frame.elements) < 4:
                return Error("LRANGE requires 3 arguments - a key, a start index for the range, and an end index for the range").encode()

            key = frame.elements[1].data
            try:
                start_index = int(frame.elements[2].data)
                end_index = int(frame.elements[3].data)
            except:
                return Error("Start and end indices must be integers").encode()
            
            async with STORE_LOCK: # Acquire asyncio Lock
                if key in STORE:
                    if isinstance(STORE[key], list):
                        values = STORE.get(key) # Retrieve the value for the key
                    else:
                        return Error("WRONGTYPE: Operation against a key holding the wrong kind of value").encode()
                else:
                    return Error("Key not found").encode()

            if start_index > end_index:
                return Array([]).encode() # Return an empty array if range is invalid

            # Translate negative indices to positive indices
            if end_index + len(values) < 0: # Entire range is out-of-bounds
                return Array([]).encode() 
            elif end_index < 0: # Partial range is out-of-bounds
                start_index = max(0, start_index + len(values))
                end_index = end_index + len(values)

            result = values[start_index:end_index + 1]

            return Array([BulkString(value) for value in result]).encode()

        elif command == "EXISTS":
            # Handle EXISTS command
            if len(frame.elements) < 2:
                return Error("EXISTS requires at least one key").encode()
            
            keys = [element.data for element in frame.elements[1:]]
            exists_count = 0
            async with STORE_LOCK: # Acquire asyncio lock
                for key in keys:
                    if key in STORE:
                        exists_count += 1
            
            return SimpleString(f"(integer) {exists_count}").encode()

        elif command == "INCR":
            # Handle INCR command
            if len(frame.elements) != 2:
                return Error("ERR wrong number of arguments for command").encode()
            
            key = frame.elements[1].data
            async with STORE_LOCK: # Acquire Asyncio lock
                if key in STORE:
                    if isinstance(STORE[key], tuple) and STORE.get(key)[0].lstrip('-+').isdigit():
                        value, expiry_time = STORE.get(key)
                        STORE[key] = (str(int(value) + 1), expiry_time)
                        return SimpleString(f"(integer) {int(value) + 1}").encode()
                    else:
                        return Error("Value is not an integer or out of range").encode()
                else:
                    STORE[key] = ("1", None)
                    return SimpleString(f"(integer) 1").encode()

        elif command == "DECR":
            # Handle INCR command
            if len(frame.elements) != 2:
                return Error("ERR wrong number of arguments for command").encode()
            
            key = frame.elements[1].data
            async with STORE_LOCK: # Acquire Asyncio lock
                if key in STORE:
                    if isinstance(STORE[key], tuple) and STORE.get(key)[0].lstrip('-+').isdigit():
                        value, expiry_time = STORE.get(key)
                        STORE[key] = (str(int(value) - 1), expiry_time)
                        return SimpleString(f"(integer) {int(value) - 1}").encode()
                    else:
                        return Error("Value is not an integer or out of range").encode()
                else:
                    STORE[key] = ("-1", None)
                    return SimpleString(f"(integer) -1").encode()

        else:
            return Error("Invalid command").encode()

    return Error("Invalid frame type").encode()

async def expiry_scheduler(STORE, STORE_LOCK):
    """Background task to delete expired keys"""
    # Run indefinitely
    while True:
        # Step 1: Get keys with expiry
        async with STORE_LOCK:
            keys_with_expiry = [
                key for key, value in STORE.items()
                if isinstance(value, tuple) and value[1] is not None
            ]

        # If No keys with expiry, wait for 100ms before checking again
        if not keys_with_expiry:
            await asyncio.sleep(0.1)
            continue

        # Step 2: Sample 20 keys at random
        sampled_keys = random.sample(keys_with_expiry, min(20, len(keys_with_expiry)))
        
        # Step 3: Check for expired keys
        expired_key_count = 0
        async with STORE_LOCK:
            for key in sampled_keys:
                _, expiry_time = STORE.get(key)
                if expiry_time is not None and time.time() > expiry_time:
                    expired_key_count += 1
                    del STORE[key]

        # Step 4: Restart if more than 25% of sampled keys are 
        if expired_key_count > 0.25 * len(sampled_keys):
            continue

        # Step 4: Wait 100ms before next check
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    try:
        print("Using Asyncio")
        STORE: dict = {}
        STORE_LOCK = asyncio.Lock()
        
        async def gather_all_async_tasks():
            await asyncio.gather(
            start_server_using_asyncio(STORE, STORE_LOCK),
            expiry_scheduler(STORE, STORE_LOCK)
        )
        asyncio.run(gather_all_async_tasks())
    except KeyboardInterrupt:
        print("Server shutting down...")