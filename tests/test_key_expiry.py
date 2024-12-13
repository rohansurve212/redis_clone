import asyncio

async def main():
    reader, writer = await asyncio.open_connection('127.0.0.1', 7)

    # Set a key with expiry
    writer.write(b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$13\r\nHello, Redis!\r\n$2\r\nEX\r\n$1\r\n5\r\n")
    await writer.drain()
    print(await reader.read(1024))

    # Get the key immediately
    writer.write(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n")
    await writer.drain()
    print(await reader.read(1024))

    # Wait for 6 seconds and try again
    await asyncio.sleep(6)
    writer.write(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n")
    await writer.drain()
    print(await reader.read(1024))

    writer.close()
    await writer.wait_closed()

asyncio.run(main())