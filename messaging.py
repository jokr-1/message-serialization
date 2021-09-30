# coding: utf-8
import struct
import re
import json
import hashlib
import textwrap
import zlib
from typing import *

"""
Messaging serialization useful e.g. in combination with zeromq sockets

There are three main concepts:

1. Declarative definition of messages, i.e.

    @register(b'range-request')  # header of message
    class RangeRequest(Message): # messages must be subclasses of Message
        start_idx = '<I'  # message-fields, consisting label and struct.pack/unpack format
        count = '<I'

2. Registering of a new Message

    Registers a message in the messaging system by looking at the message declaration and building appropriate
    serializing and deserializing structures (Message._type and Message._fields).
    These structures are class variables and thus do not impact each message instance. Hence, the run-time overhead is 
    negligible (neɡlɪdʒəb(ə)l).

3. (de-)serialization
    serialize(msg: Message) turns a Message into a list, where each message-field is automatically serialized depending
    on the given format string:
        'json': json.dumps(field[idx]).encode('utf-8')
        'blob': field[idx] 
        '<I': struct.pack('<I', field[idx])
    
    Additionally, the Ellipsis object (...) can be used as a placeholder for any additional elements (in the form of bytes).
    
    deserialize(msg) turns a list into a Message, where each element the concrete message subtype is determined using 
    the first element in the list and the remaining fields are deserialized according to the format string in the 
    message-fields:
        'json': json.loads(msg[idx].decode('utf-8'))
        'blob': msg[idx]
        '<I': struct.unpack('<I', msg[idx])

"""

MSGTYPE_HASH_LENGTH = 4
message_ctors = dict()

class MessageError(Exception):
    """ Base class for errors in messaging system """
    pass

class UnknownMessageError(MessageError):
    """ Error if message type is unknown """
    pass



def constructor(type_hash):
    return message_ctors[type_hash]


def deserialize_message(msg):
    type_hash = msg[:MSGTYPE_HASH_LENGTH]
    try:
        ctor = message_ctors[type_hash] # TODO: Eigenen Error-Type für unbekannte Messages
        msg = ctor(msg)
        return msg
    except KeyError:
        raise UnknownMessageError(f'Unknown message-type hash: {type_hash}.') from None


def get_type_hash(msg_raw):
    return msg_raw[:MSGTYPE_HASH_LENGTH]


def register(msgtype_hash=None, compress=False):
    def do_register(cls):
        fields = dict()
        names = list(enumerate(filter(lambda f: not f.startswith('_'), cls.__dict__.keys())))

        def to_field(fmt):
            if fmt == 'json':
                return JsonField()
            elif fmt == 'blob':
                return BlobField()
            elif fmt == 'utf-8':
                return Utf8Field()
            elif fmt == 'boolean':
                return BooleanField()
            elif isinstance(fmt, dict):
                return DictField({field_name: to_field(field_fmt) for field_name, field_fmt in fmt.items()})
            elif isinstance(fmt, str):
                return StructField(fmt)
            elif isinstance(fmt, list):
                assert len(fmt) == 1
                return ListField(to_field(fmt[0]))
            elif Message in fmt.__bases__:
                return MessageField(fmt)

        for i, field_name in names:
            fmt = cls.__dict__[field_name]
            field = to_field(fmt)
            fields[field_name] = field
            delattr(cls, field_name)

        cls._fields = fields

        # create messsagetype-hash from message-definition
        if msgtype_hash is not None:
            if not isinstance(msgtype_hash, bytes) or len(msgtype_hash) != MSGTYPE_HASH_LENGTH:
                raise MessageError(f'Messagetype-Hash must be bytes of length {MSGTYPE_HASH_LENGTH}.')
            typehash = msgtype_hash
        else:
            clsdefinition = cls.definition().encode('utf-8')
            typehash = hashlib.blake2b(clsdefinition, digest_size=MSGTYPE_HASH_LENGTH).digest()


        if typehash in message_ctors:
            raise MessageError(f'Hash collision in message definitions. Messagetype-Hash {typehash} is already defined by another message ({message_ctors[typehash].__name__}).')

        cls._typehash = typehash
        cls._compress = compress
        message_ctors[cls._typehash] = cls
        return cls
    return do_register


class Message:
    _typehash = None
    _fields = None
    _compress = False

    def __init__(self, payload):
        self.payload = payload
        self._cache = None

    def __getattr__(self, item):
        if self._cache is None:
            self._cache = dict()
            buffer = self.payload[MSGTYPE_HASH_LENGTH:]
            if self._compress:
                buffer = zlib.decompress(buffer)
            self.deserialize(buffer)
        return self._cache[item]

    def deserialize(self, buffer):
        length = len(buffer)
        offset = 0
        for field_name, field in self._fields.items():
            value, offset = field.deserialize(buffer, offset)
            self._cache[field_name] = value
        assert offset == length

    @classmethod
    def serialize(cls, **kwargs):
        msg = b''
        for field_name in cls._fields:
            value = kwargs[field_name]
            field = cls._fields[field_name]
            msg += field.serialize(value)
        if cls._compress:
            msg = zlib.compress(msg)
        return cls._typehash + msg

    @classmethod
    def definition(cls):
        header = f'class {cls.__name__}(Message):'
        body = []
        for field_name, field in cls._fields.items():
            body.append(f'{field_name} = {field}')
        body = '\n'.join(body)
        cls_def = f'{header}\n{textwrap.indent(body, "    ")}'
        return cls_def

    def __str__(self):
        max_field_len = 20
        fields = []
        for field_name in self._fields.keys():
            value = str(getattr(self, field_name))
            fields.append(f'{field_name}={value}')
        fields = ', '.join(fields)
        return f'<{self.__class__.__name__} {fields}>'


class UnknownMessage:
    def __init__(self, msg):
        self.msg = msg


class Field:

    def serialize(self, value) -> bytes:
        raise NotImplementedError

    def deserialize(self, buffer: bytes, offset: int) -> (object, int):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError


class BooleanField(Field):

    def serialize(self, value):
        return struct.pack('<?', value)

    def deserialize(self, buffer, offset):
        return struct.unpack_from('<?', buffer, offset)[0], offset + 1

    def __str__(self):
        return "'boolean'"


class StructField(Field):

    def __init__(self, fmt):
        self.fmt = fmt
        self.nitems = len(re.sub("[0-9@=<>!]", "", fmt))
        self.size = struct.calcsize(self.fmt)

    def serialize(self, values):
        if self.nitems > 1:
            return struct.pack(self.fmt, *values)
        return struct.pack(self.fmt, values)

    def deserialize(self, buffer, offset):
        data = struct.unpack_from(self.fmt, buffer, offset)
        if self.nitems == 1:
            data = data[0]
        return data, offset+self.size

    def __str__(self):
        return f"'{self.fmt}'"


class BlobField(Field):

    def serialize(self, value):
        return struct.pack('<I', len(value)) + value

    def deserialize(self, buffer, offset):
        sz = struct.calcsize('<I')
        length = struct.unpack_from('<I', buffer, offset)[0]
        return buffer[offset+sz:offset+sz+length], offset+sz+length

    def __str__(self):
        return "'blob'"


class Utf8Field(Field):

    def serialize(self, value: str):
        return struct.pack('<I', len(value)) + value.encode('utf-8')

    def deserialize(self, buffer, offset):
        sz = struct.calcsize('<I')
        length = struct.unpack_from('<I', buffer, offset)[0]
        return buffer[offset+sz:offset+sz+length].decode('utf-8'), offset+sz+length

    def __str__(self):
        return "'utf-8'"


class JsonField(Field):

    def serialize(self, value):
        dumped = json.dumps(value).encode('utf-8')
        return struct.pack('<I', len(dumped)) + dumped

    def deserialize(self, buffer, offset):
        sz = struct.calcsize('<I')
        length = struct.unpack_from('<I', buffer, offset)[0]
        json_data = buffer[offset+sz:offset+sz+length]
        return json.loads(bytes(json_data)), offset+sz+length

    def __str__(self):
        return "'json'"

class DictField(Field):

    def __init__(self, nested: Dict[str, Field]):
        self.nested = nested

    def serialize(self, value) -> bytes:
        msg = b''
        for field_name, field in self.nested.items():
            msg += field.serialize(value[field_name])
        return msg

    def deserialize(self, buffer, offset):
        obj = {}
        for field_name, field in self.nested.items():
            value, offset = field.deserialize(buffer, offset)
            obj[field_name] = value
        return obj, offset

    def __str__(self):
        items = []
        for field_name, field in self.nested.items():
            items.append(f"'{field_name}': {field},")
        items = '\n'.join(items)
        return f'{{\n{textwrap.indent(items, "    ")}\n}}'


class ListField(Field):

    def __init__(self, elem_field: Field):
        self.elem_field = elem_field

    def serialize(self, values):
        msg = struct.pack('<I', len(values))
        for value in values:
            msg += self.elem_field.serialize(value)
        return msg

    def deserialize(self, buffer, offset):
        sz = struct.calcsize('<I')
        count = struct.unpack_from('<I', buffer, offset)[0]
        offset = offset+sz
        values = []
        for i in range(count):
            value, offset = self.elem_field.deserialize(buffer, offset)
            values.append(value)
        return values, offset

    def __str__(self):
        return f'[{self.elem_field}]'


class MessageField(Field):

    def __init__(self, msg_cls):
        self.msg_cls = msg_cls

    def serialize(self, value):
        data = value
        return struct.pack('<I', len(data)) + data

    def deserialize(self, buffer, offset):
        sz = struct.calcsize('<I')
        length = struct.unpack_from('<I', buffer, offset)[0]
        msg = self.msg_cls(buffer[offset+sz:offset+sz+length])
        return msg, offset+sz+length

    def __str__(self):
        return self.msg_cls.__name__
