# MessageStream

## Overview

MessageStream, as the name suggests, is designed to send a series of messages.  It's intended as a binary alternative to
[Json](https://www.json.org/json-en.html) which should be simpler to use for serializing application defined classes and
other custom types [MessagePack](https://msgpack.org/index.html).

The basic premise is that applications provide their own callbacks to decompose custom types into MessageStream inbuilt
types while encoding a message and likewise provide callbacks to reassemble the same types while decoding a message.
Type names and object key names must be pre-agreed by the applications just as key names are pre-agreed in json.

If types have not been pre-agreed, the decoding application can still decode the message.  The application will just 
receive the information in decomposed inbuilt types just as they would in Json.

## Streams, Messages and Objects

A Stream is a sequence of messages in in a strict order.  If applications wish to send unordered (eg: over UDP) messages 
then this can be achieved by starting a new MessageStream for each message incurring a small overhead.
See [Unordered Messages](#unordered-messages).

Every message is represented by a single object which may itself contain other objects.

Top-level objects always start with a [control-code](#control-codes) indicating the encoding and type, the rest of the
object's encoding follows as per the [control-code's definition](#control-code-definitions).

## Out of Band Instructions

FIX ME

A necessary feature of this standard is to allow some additional meta-information to be conveyed.  This is primarily
to avoid 
throughout a message.  More precisely out of band control codes can be placed anywhere that another control code was 
expected.  The expected control-code MUST follow the out of band control-code and any associated objects.

 - Defining a new custom type.
 - Flagging that the following object may be referred to later (anchor)
 - Enabling / disabling automatic anchors
 - Defining an object which has been mentioned in a forward reference
 
 Out of band control codes are effectively invisible to the flow rest of the flow.  The *do* something and may augment 
 the meaning, but they have no effect on the structure. 

## Inbuilt types

MessageStream defines types separately from encodings for types.  Some types have multiple encodings.

- Null (AKA python None)
- Boolean (True / False)
- Positive Integer
- Integer (Positive and negative)
- Decimal (Floating point number which behaves as if calculated in base 10)
- Floating point
- String (Always UTF-8)
- Date / Time
- Raw Bytes
- List
- Key-Value map

## Custom types

Custom types are achieved by first writing an out-of-band type definition. 

The encoding for custom types may be defined either as a direct reference to an inbuilt type or a pre-defined structure 
built of custom types.  Custom types SHOULD avoid the trap of just encoding everything to bytes and using "Raw Bytes" as
it's encoding.

One specific rule on Integers is that when defining the custome type

## Variable Byte Integers

Control-codes themselves and a few primative data types make use of Variable Byte Integers.

All variable byte integers are positive integers with a variable byte encoding containing 1, 2, 4, or 8 
bytes.  The number of bytes in the control number is dictated by the most significant 1 to 4 bits of the first byte.  
All remaining bits represent an integer with the most significant bits first (Bigendian):

| First Four Bits | Number of bytes | Min to Max value |
|-----------------|-----------------|-----------------|
| `0xxxxxxx`      | 1 Byte          | 0 to 2<sup>7</sup>-1 = 127  |
| `10xxxxxx`      | 2 Bytes         | 128 to 2<sup>14</sup>-1 = 16,383  |
| `110xxxxx`      | 4 Bytes         | 16384 to 2<sup>29</sup>-1 = 53,687,0911 |
| `1110xxxx`      | 8 Bytes         | 53,687,0912 to 2<sup>60</sup>-1|

When encoding control numbers, encoding a control number below the minimum specified above is as [Protcol Error](#protocol-error).
When decoding control numbers, it is at the decoder's descretion whether or not to allow it.  

This means the decoder is not required to check, but the encoder MUST not bloat a message with unecessary bytes.

## Alternative Encodings

Some inbuilt types have alternative encodings.  Consiquently custom types will also have alternative encodings.  
In all situations encoders are free to make their own decisions on which encoding to use for a type.  
Even where an encoding is explicitly mentioned by a Custom Type, an encoder may use any suitable alternative for that 
type and a decoder MUST accept any alternative for the same type.


## Control codes

| Type    | Code   | Name                                      |
|---------|--------|-------------------------------------------|
|         | 0      | Stop Code
|         | 1      | Strict Struct Type Definition             |
|         | 2      | Flexible Struct Type Definition           | 
|         | 3      | 
|         | 1      | Reference Anchor |
|         | 2      | Enable Anchorless References |  After this point in the stream, every object is automatically assigned an anchor
|         | 3      | Disable Anchorless references | Used after `2` to stop assigning anchors in the stream.
|         | 4      | Back Reference   | Used in place of any other object to refer back to a *fully* described object.
|         | 5      | Forward Reference | Used in place of any other object to refer to a *partially* described object.
|         | 6      | Complete forward reference | Out of band op-code to write an arbitrary object.  This is useful if a forward reference has been used and there's no good palce to put the forward reference object. |
| Null    | 10     | Null  | Null AKA None none python.
| Bool    | 11     | False | Boolean false
| Bool    | 12     | True  | Boolean true
| Int     | 13     | Positive Variable byte Int | Define a positive integer by variable byte
| Int     | 14     | Negative Variable bytee Int | Define a negative integer by variable byte
| Int     | 15     | Signed multi byte integer | Control-code is followed by a variable byte integer stating how many bytes and the that number of bytes representing the integer as Bigendian
| Str     | 16     | String | Unicode String encoded as UTF 8.  Control-code is immediately followed 

## Anchors and References

MessageStream supports reusing objects.  An object needs to be flagged with an anchor and then it may be referenced with either a forward or backward reference.

 - Backward references are used at a point in the message where the referenced object is fully defined.  
 - Forward references are used when the referenced object is not yet completely defined.  *Note that this definition does 
not care about linked or contained objects, it's the referenced object itself that matters.*

For this purpose, any object can be prefixed with an anchor control code, or anchorless references can be enabled.
The effect of enabling anchorless references is to implicitly add an anchor to every following object and these anchors 
remain usable even after anchorless references have been disabled.

All anchors only survive for a message (other wise recipients would have to store every anchored ovbject they'd ever seen).

If the implementation maps a type to a mutable object then it MUST implement references to that type as a reference to 
the same mutable object and not a carbon copy.

## Protocol Error

Throughout this standard, there are numberous errors to Protocol Errors.  Decoders are required to check for all 
protocol errors unless otherwise sepcified.  If a protocol error is found while decoding, the decoder must stop decoding 
and abandon any partially parsed message.  There is no way to re-sync decoding after a protocol error so any remaining 
bytes in the stream must also be abandoned without processing.

*Note: When encountering a Protocol Error on a two-way communications protocol it would be usual to return an error 
back to the client before closing the connection.  Specification of such behaviour is beyond the scope of this
standard.*

## Control-code definitions

### Type Definition

The Stop code is is used as an end marker

## Considderations

### Unordered Messages
