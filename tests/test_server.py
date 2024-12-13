import pytest
from pyredis.protocol import Array, BulkString, SimpleString, Error
from pyredis.server import process_command

STORE: dict = {}

def test_command():
    frame = Array([BulkString("COMMAND")])
    response = process_command(frame)
    assert response == b"$2\r\nOK\r\n"

def test_info_server():
    frame = Array([BulkString("INFO"), BulkString("SERVER")])
    response = process_command(frame)
    assert response == b"$29\r\n# Server\nredis_version:0.1.0\n\r\n"

def test_info_other():
    frame = Array([BulkString("INFO")])
    response = process_command(frame)
    assert response == b"$0\r\n\r\n"

def test_echo_single_word():
    frame = Array([BulkString("ECHO"), BulkString("Hello")])
    response = process_command(frame)
    assert response == b"$5\r\nHello\r\n"

def test_echo_multiple_words():
    frame = Array([BulkString("ECHO"), BulkString("Hello"), BulkString("Redis"), BulkString("World!")])
    response = process_command(frame)
    assert response == b"$18\r\nHello Redis World!\r\n"

def test_echo_missing_argument():
    frame = Array([BulkString("ECHO")])
    response = process_command(frame)
    assert response == b"-ECHO requires an argument\r\n"

def test_ping_no_arguments():
    frame = Array([BulkString("PING")])
    response = process_command(frame)
    assert response == b"+PONG\r\n"

def test_ping_with_argument():
    frame = Array([BulkString("PING"), BulkString("Hello")])
    response = process_command(frame)
    assert response == b"+Hello\r\n"

def test_invalid_command():
    frame = Array([BulkString("INVALID")])
    response = process_command(frame)
    assert response == b"-Invalid command\r\n"

def test_empty_array():
    frame = Array([])
    response = process_command(frame)
    assert response == b"-Empty command\r\n"

def test_invalid_frame_type():
    frame = SimpleString("Invalid")
    response = process_command(frame)
    assert response == b"-Invalid frame type\r\n"

def test_set_command():
    frame = Array([BulkString("SET"), BulkString("key"), BulkString("value")])
    response = process_command(frame)
    assert response == b"+OK\r\n"

def test_get_command():
    STORE["key"] = "value"
    frame = Array([BulkString("GET"), BulkString("key")])
    response = process_command(frame)
    assert response == b"$5\r\nvalue\r\n"