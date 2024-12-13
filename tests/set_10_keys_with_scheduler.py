import asyncio

async def test_set_10_keys_with_scheduler():
    """Test setting 10 keys with varying expiry and validate scheduler cleanup."""
    reader, writer = await asyncio.open_connection('127.0.0.1', 7)

    # Set 10 keys with varying expiry times
    for i in range(10):
        key = f"key{i}"
        value = f"value{i}"
        expiry = (i % 5000) + 1  # Expiry times: 1 to 5 seconds
        command = f"*5\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n$2\r\nEX\r\n${len(str(expiry))}\r\n{expiry}\r\n"
        writer.write(command.encode())
        await writer.drain()
        response = await reader.read(1024)
        print(f"SET key{i}")
        assert response == b"+OK\r\n"

    print("10 keys set with varying expiry times.")

    # Check keys before expiry
    for i in range(10):
        key = f"key{i}"
        command = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n"
        writer.write(command.encode())
        await writer.drain()
        response = await reader.read(1024)
        print(f"GET response for key{i} before expiry - {response}")
        assert response == f"${len(f'value{i}')}\r\nvalue{i}\r\n".encode()
    print("Verified keys before expiry.")

    # Wait for 6 seconds to ensure all keys are expired
    await asyncio.sleep(6)

    # Check keys after expiry
    expired_count = 0
    for i in range(10):
        key = f"key{i}"
        command = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n"
        writer.write(command.encode())
        await writer.drain()
        response = await reader.read(1024)
        print(f"GET response for key{i} after expiry - {response}")
        if response == b"$-1\r\n":  # Null response for expired keys
            expired_count += 1

    print(f"{expired_count} keys were cleaned up by the scheduler.")

    # Validate all keys have been cleaned
    assert expired_count == 10, f"Expected all 10 keys to be expired, but only {expired_count} were."

    # Clean up
    writer.close()
    await writer.wait_closed()
    print("Test completed successfully.")

asyncio.run(test_set_10_keys_with_scheduler())
