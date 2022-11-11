# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: gripper.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='gripper.proto',
  package='gripper',
  syntax='proto3',
  serialized_options=b'Z\034github.com/bmeg/grip/gripper',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\rgripper.proto\x12\x07gripper\x1a\x1cgoogle/protobuf/struct.proto\"\x07\n\x05\x45mpty\"\x1a\n\nCollection\x12\x0c\n\x04name\x18\x01 \x01(\t\"\x13\n\x05RowID\x12\n\n\x02id\x18\x01 \x01(\t\"?\n\nRowRequest\x12\x12\n\ncollection\x18\x01 \x01(\t\x12\n\n\x02id\x18\x02 \x01(\t\x12\x11\n\trequestID\x18\x03 \x01(\x04\"@\n\x0c\x46ieldRequest\x12\x12\n\ncollection\x18\x01 \x01(\t\x12\r\n\x05\x66ield\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t\"K\n\x03Row\x12\n\n\x02id\x18\x01 \x01(\t\x12%\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x17.google.protobuf.Struct\x12\x11\n\trequestID\x18\x03 \x01(\x04\"\x8f\x01\n\x0e\x43ollectionInfo\x12\x15\n\rsearch_fields\x18\x01 \x03(\t\x12\x36\n\x08link_map\x18\x02 \x03(\x0b\x32$.gripper.CollectionInfo.LinkMapEntry\x1a.\n\x0cLinkMapEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x32\xd8\x02\n\nGRIPSource\x12\x37\n\x0eGetCollections\x12\x0e.gripper.Empty\x1a\x13.gripper.Collection0\x01\x12\x41\n\x11GetCollectionInfo\x12\x13.gripper.Collection\x1a\x17.gripper.CollectionInfo\x12/\n\x06GetIDs\x12\x13.gripper.Collection\x1a\x0e.gripper.RowID0\x01\x12.\n\x07GetRows\x12\x13.gripper.Collection\x1a\x0c.gripper.Row0\x01\x12\x34\n\x0bGetRowsByID\x12\x13.gripper.RowRequest\x1a\x0c.gripper.Row(\x01\x30\x01\x12\x37\n\x0eGetRowsByField\x12\x15.gripper.FieldRequest\x1a\x0c.gripper.Row0\x01\x42\x1eZ\x1cgithub.com/bmeg/grip/gripperb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_struct__pb2.DESCRIPTOR,])




_EMPTY = _descriptor.Descriptor(
  name='Empty',
  full_name='gripper.Empty',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=56,
  serialized_end=63,
)


_COLLECTION = _descriptor.Descriptor(
  name='Collection',
  full_name='gripper.Collection',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='gripper.Collection.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=65,
  serialized_end=91,
)


_ROWID = _descriptor.Descriptor(
  name='RowID',
  full_name='gripper.RowID',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='gripper.RowID.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=93,
  serialized_end=112,
)


_ROWREQUEST = _descriptor.Descriptor(
  name='RowRequest',
  full_name='gripper.RowRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='collection', full_name='gripper.RowRequest.collection', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='id', full_name='gripper.RowRequest.id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='requestID', full_name='gripper.RowRequest.requestID', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=114,
  serialized_end=177,
)


_FIELDREQUEST = _descriptor.Descriptor(
  name='FieldRequest',
  full_name='gripper.FieldRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='collection', full_name='gripper.FieldRequest.collection', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='field', full_name='gripper.FieldRequest.field', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='gripper.FieldRequest.value', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=179,
  serialized_end=243,
)


_ROW = _descriptor.Descriptor(
  name='Row',
  full_name='gripper.Row',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='gripper.Row.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='data', full_name='gripper.Row.data', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='requestID', full_name='gripper.Row.requestID', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=245,
  serialized_end=320,
)


_COLLECTIONINFO_LINKMAPENTRY = _descriptor.Descriptor(
  name='LinkMapEntry',
  full_name='gripper.CollectionInfo.LinkMapEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='gripper.CollectionInfo.LinkMapEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='gripper.CollectionInfo.LinkMapEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=420,
  serialized_end=466,
)

_COLLECTIONINFO = _descriptor.Descriptor(
  name='CollectionInfo',
  full_name='gripper.CollectionInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='search_fields', full_name='gripper.CollectionInfo.search_fields', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='link_map', full_name='gripper.CollectionInfo.link_map', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_COLLECTIONINFO_LINKMAPENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=323,
  serialized_end=466,
)

_ROW.fields_by_name['data'].message_type = google_dot_protobuf_dot_struct__pb2._STRUCT
_COLLECTIONINFO_LINKMAPENTRY.containing_type = _COLLECTIONINFO
_COLLECTIONINFO.fields_by_name['link_map'].message_type = _COLLECTIONINFO_LINKMAPENTRY
DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY
DESCRIPTOR.message_types_by_name['Collection'] = _COLLECTION
DESCRIPTOR.message_types_by_name['RowID'] = _ROWID
DESCRIPTOR.message_types_by_name['RowRequest'] = _ROWREQUEST
DESCRIPTOR.message_types_by_name['FieldRequest'] = _FIELDREQUEST
DESCRIPTOR.message_types_by_name['Row'] = _ROW
DESCRIPTOR.message_types_by_name['CollectionInfo'] = _COLLECTIONINFO
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Empty = _reflection.GeneratedProtocolMessageType('Empty', (_message.Message,), {
  'DESCRIPTOR' : _EMPTY,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.Empty)
  })
_sym_db.RegisterMessage(Empty)

Collection = _reflection.GeneratedProtocolMessageType('Collection', (_message.Message,), {
  'DESCRIPTOR' : _COLLECTION,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.Collection)
  })
_sym_db.RegisterMessage(Collection)

RowID = _reflection.GeneratedProtocolMessageType('RowID', (_message.Message,), {
  'DESCRIPTOR' : _ROWID,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.RowID)
  })
_sym_db.RegisterMessage(RowID)

RowRequest = _reflection.GeneratedProtocolMessageType('RowRequest', (_message.Message,), {
  'DESCRIPTOR' : _ROWREQUEST,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.RowRequest)
  })
_sym_db.RegisterMessage(RowRequest)

FieldRequest = _reflection.GeneratedProtocolMessageType('FieldRequest', (_message.Message,), {
  'DESCRIPTOR' : _FIELDREQUEST,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.FieldRequest)
  })
_sym_db.RegisterMessage(FieldRequest)

Row = _reflection.GeneratedProtocolMessageType('Row', (_message.Message,), {
  'DESCRIPTOR' : _ROW,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.Row)
  })
_sym_db.RegisterMessage(Row)

CollectionInfo = _reflection.GeneratedProtocolMessageType('CollectionInfo', (_message.Message,), {

  'LinkMapEntry' : _reflection.GeneratedProtocolMessageType('LinkMapEntry', (_message.Message,), {
    'DESCRIPTOR' : _COLLECTIONINFO_LINKMAPENTRY,
    '__module__' : 'gripper_pb2'
    # @@protoc_insertion_point(class_scope:gripper.CollectionInfo.LinkMapEntry)
    })
  ,
  'DESCRIPTOR' : _COLLECTIONINFO,
  '__module__' : 'gripper_pb2'
  # @@protoc_insertion_point(class_scope:gripper.CollectionInfo)
  })
_sym_db.RegisterMessage(CollectionInfo)
_sym_db.RegisterMessage(CollectionInfo.LinkMapEntry)


DESCRIPTOR._options = None
_COLLECTIONINFO_LINKMAPENTRY._options = None

_GRIPSOURCE = _descriptor.ServiceDescriptor(
  name='GRIPSource',
  full_name='gripper.GRIPSource',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=469,
  serialized_end=813,
  methods=[
  _descriptor.MethodDescriptor(
    name='GetCollections',
    full_name='gripper.GRIPSource.GetCollections',
    index=0,
    containing_service=None,
    input_type=_EMPTY,
    output_type=_COLLECTION,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetCollectionInfo',
    full_name='gripper.GRIPSource.GetCollectionInfo',
    index=1,
    containing_service=None,
    input_type=_COLLECTION,
    output_type=_COLLECTIONINFO,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetIDs',
    full_name='gripper.GRIPSource.GetIDs',
    index=2,
    containing_service=None,
    input_type=_COLLECTION,
    output_type=_ROWID,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetRows',
    full_name='gripper.GRIPSource.GetRows',
    index=3,
    containing_service=None,
    input_type=_COLLECTION,
    output_type=_ROW,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetRowsByID',
    full_name='gripper.GRIPSource.GetRowsByID',
    index=4,
    containing_service=None,
    input_type=_ROWREQUEST,
    output_type=_ROW,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetRowsByField',
    full_name='gripper.GRIPSource.GetRowsByField',
    index=5,
    containing_service=None,
    input_type=_FIELDREQUEST,
    output_type=_ROW,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_GRIPSOURCE)

DESCRIPTOR.services_by_name['GRIPSource'] = _GRIPSOURCE

# @@protoc_insertion_point(module_scope)
