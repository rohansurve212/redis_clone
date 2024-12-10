# Redis Protocol:
# Spec: https://redis.io/docs/latest/develop/reference/protocol-spec/

# For Simple Strings, the first byte of the reply is "+"     "+OK\r\n"
# For Errors, the first byte of the reply is "-"             "-Error message\r\n"
# For Integers, the first byte of the reply is ":"           ":[<+|->]<value>\r\n"
# For Bulk Strings, the first byte of the reply is "$"       "$<length>\r\n<data>\r\n"
# For Arrays, the first byte of the reply is "*"             "*<number-of-elements>\r\n<element-1>...<element-n>"


# We will need a module to extract messages from the stream.
# When we read from the network we will get:
# 1. A partial message.
# 2. A whole message.
# 3. A whole message, followed by either 1 or 2.
# We will need to remove parsed bytes from the stream.
from pyredis.protocol import BulkString, Error, parse_frame, SimpleString, Integer, Array

import pytest


@pytest.mark.parametrize("buffer, expected", [
    # Simple String
    (b"+OK", (None, 0)),
    (b"+OK\r\n", (SimpleString("OK"), 5)),
    (b"+OK\r\n+Part", (SimpleString("OK"), 5)),
    (b"+OK\r\n+Second Message\r\n", (SimpleString("OK"), 5)),
    # Error
    (b"-Err", (None, 0)),
    (b"-Error\r\n", (Error("Error"), 8)),
    (b"-Error\r\n-Part Error", (Error("Error"), 8)),
    # BulkString
    (b"$5\r\nPart", (None, 0)),
    (b"$11\r\nBulk String\r\n", (BulkString("Bulk String"), 18)),
    (b"$8\r\nBulk Str\r\n$+OK", (BulkString("Bulk Str"), 14)),
    # Integer
    (b":1000\r\n", (Integer(1000), 7)),
    #Array
    (b"*-1\r\n", (Array(None), 5)), # Null Array
    (b"*2\r\n+OK\r\n:1000\r\n", (Array([SimpleString("OK"), Integer(1000)]), 16)),
    (b"*3\r\n+Simple\r\n:42\r\n$6\r\nString\r\n", (
        Array([
            SimpleString("Simple"),
            Integer(42),
            BulkString("String")
        ]), 30
    ))
])
def test_parse_frame(buffer, expected):
    got = parse_frame(buffer)
    assert got[0] == expected[0]
    assert got[1] == expected[1]

def test_serialise_simplestring():
    val = SimpleString("OK")
    expected = b"+OK\r\n"
    assert val.encode() == expected

def test_serialize_error():
    val = Error("Error")
    expected = b"-Error\r\n"
    assert val.encode() == expected

def test_serialise_bulkstring():
    # Null Bulk String
    val = BulkString(None)
    expected = b"$-1\r\n"
    assert val.encode() == expected

    # Empty Bulk String
    val = BulkString("")
    expected = b"$0\r\n\r\n"
    assert val.encode() == expected
    
    # Non-null Bulk String
    val = BulkString("Rohan Surve")
    expected = b"$11\r\nRohan Surve\r\n"
    assert val.encode() == expected
    
    # Bulk String with special characters
    val = BulkString("Line1\nLine2")
    expected = b"$11\r\nLine1\nLine2\r\n"
    assert val.encode() == expected

def test_serialize_integer():
    # Positive Integer
    val = Integer(1000)
    expected = b":1000\r\n"
    assert val.encode() == expected
    
    # Negative Integer
    val = Integer(-123)
    expected = b":-123\r\n"
    assert val.encode() == expected
    
    # Zero Integer
    val = Integer(0)
    expected = b":0\r\n"
    assert val.encode() == expected

def test_serialize_Array():
    # Test Case 1
    val = Array([
            SimpleString("Simple"),
            Integer(42),
            BulkString("String")
        ])
    expected = b"*3\r\n+Simple\r\n:42\r\n$6\r\nString\r\n"
    assert val.encode() == expected
    
    # Test Case 2
    val = Array([
            Error("Error"),
            Error(""),
            BulkString("String")
        ])
    expected = b"*3\r\n-Error\r\n-\r\n$6\r\nString\r\n"
    assert val.encode() == expected