from dataclasses import dataclass
from typing import List, Union


@dataclass
class SimpleString:
    data: str

    def encode(self):
        return f"+{self.data}\r\n".encode()

@dataclass
class Error:
    message: str

    def encode(self):
        return f"-{self.message}\r\n".encode()

@dataclass
class BulkString:
    data: str
    
    def encode(self):
        if self.data is None: # Null Bulk String
            return b"$-1\r\n"
        else:
            # Non-null Bulk String
            encoded_data = self.data.encode()
            length = len(encoded_data)
            return f"${length}\r\n".encode() + encoded_data + b"\r\n"

@dataclass
class Integer:
    data: int
    
    def encode(self):
        return f":{self.data}\r\n".encode()

@dataclass
class Array:
    elements: List[Union[SimpleString, Error, BulkString, Integer]]
    
    def encode(self):
        parts = [f"*{len(self.elements)}\r\n".encode()]
        for element in self.elements:
            parts.append(element.encode())
        return b"".join(parts)

def parse_frame(buffer):
    end = buffer.find(b"\r\n")
    if end == -1:
        return None, 0

    match chr(buffer[0]):
        case '+':
            return SimpleString(data=buffer[1:end].decode('ascii')), end + 2

        case '-':
            return Error(message=buffer[1:end].decode('ascii')), end + 2

        case '$':
            expected_length = int(buffer[1:end].decode('ascii'))
            size = end + expected_length + 2 + 2

            if len(buffer) >= size:
                message = buffer[end + 2:end + 2 + expected_length].decode('ascii')
                return BulkString(data=message), size

        case ':':
            return Integer(data=int(buffer[1:end].decode('ascii'))), end + 2
        
        case '*':
            num_elements = int(buffer[1:end].decode('ascii'))
            if num_elements == -1: # Null array
                return Array(elements=None), end + 2
            
            elements = []
            cursor = end + 2
            for _ in range(num_elements):
                element, consumed = parse_frame(buffer[cursor:])
                if element is None:
                    return None, 0
                elements.append(element)
                cursor += consumed

            return Array(elements=elements), cursor

    return None, 0