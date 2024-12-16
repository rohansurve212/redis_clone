import asyncio

# Define a shared lock for AOF writes
AOF_LOCK = asyncio.Lock()

async def log_to_aof(command, aof_file):
    """Log a command to the AOF file."""
    async with AOF_LOCK:  # Ensure thread-safe file writes
        with open(aof_file, "ab") as file:
            file.write(command)