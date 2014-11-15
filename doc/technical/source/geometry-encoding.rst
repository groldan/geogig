GeoGig Geometry encoding format
===============================

This is the format used for internal storage of geometries in GeoGig.

Goals
-----
The goals of this geometry serialization format are:

* Self descriptive: the format should contain enough information to reconstruct the geometry in full with no external knowledge of its
  characteristics (type, dimensionality, precision model, etc). The represented geometry has no knowledge of its Coordinate Reference System though.
* 4th-Dimension support (i.e. X,Y,Z,M): Despite the fact that the default implementation of JTS coordinates and coordinate sequences support only 2 or 2.5D coordinates,
  this geometry encoding supports 4 dimensions, all of them sharing a single precision model. In the future, this specification could be extended
  to support N dimensions and/or separate precision models for each dimension.
* Compactness: The resulting serialized form shall be measurably and considerably smaller than its equivalent WKB representation when using a fixed precision model.

Introduction
------------

GeoGig uses the `Java Topology Suite <http://tsusiatsoftware.net/jts/main.html>`_ (JTS) geometry model, which is the best
OGC's Simple Features for SQL conformant Java library.

Note however, that although JTS algorithms only work for 2-dimensional geometries, and can carry over a third (Z) ordinate,
GeoGig can store 4D geometries by using a custom implementation of JTS's ``CoordinateSequenceFactory``, allowing for
X,Y,Z,M geometries (Z is generally used for height, and M for some kind of Measurement).

GeoGig works well with JTS Precision Models, and encourages the specification of a fixed precision model whenever it makes
sense for the data, on a by feature type's geometry attribute basis. This is so for two reasons. First, it's just good practice
to know your data and use an appropriate precision for it (e.g. what's the point on having full double precision on ordinates
that represent meters on a geometry that's on a UTM projection? wouldn't 4 decimal places - a tenth of a millimeter - be good enough?).

The other, perhaps more compelling reason, is storage space saving. The geometry serialization format defined here uses variable length
integers to represent ordinates, and can compress the better the lower the number of decimal places in the precision model.

That said, single precision (i.e. ``float``) and double precision (``i.e. double``) are also supported.

When using a fixed precision model, ordinates are stored as `Variable Length Integers <https://developers.google.com/protocol-buffers/docs/encoding#varints>`_
as defined by Google's Protocol Buffers specification, by first converting the ordinate's ``double`` value to a ``long`` with a multiplication factor
that's the ``10^N``, where ``N`` is the number of decimal places in the fixed precision model. They are also stored as delta values with regard
to the previous ordinate in the same dimension, maximizing the compression.

Compared to OGC's WKB encoding, the resulting geometries are around 1/3 to 2/3 the size of the WKB representation, depending on the fixed precision factor.

Note, however, that GeoGig has a prescribed precision of 9 decimal places to compute the SHA-1 hash
of the geometry attribute values in Feature objects. This number has been deemed as good enough to avoid accidentally taking a geometry as changed
when it has not been so, but its representation comes from an external source (like an import from another format, or an editing tool) that
resulted in slight changes on the ordinate values. Given the maximum allowed fixed precision is also 9, this should not represent any risk
of precision lose.

Conventions
-----------

Formats are specified using a modified Backus-Naur notation.
Definitions generally take the form::

    <structure> := part1 part2 part3

Indicating that the structure has three parts.
The parts can be:

* Another structure, referenced by name.
* One of these specially defined structures

  .. code-block:: none

    NUL       := 0x00 (ASCII NUL character)
    SP        := 0x20 (ASCII space character)
    BR        := 0x0a (ASCII newline character)
    <rev>     := <byte>* (exactly 20 bytes)
    <utf8>    := <int16> <byte>* (two-byte count followed by the number
				  of bytes indicated by the count. 
				  These should then be decoded as 
				  modified UTF-8, as seen in the 
				  readUTF and writeUTF methods in the
    				  java.io.DataInputStream and
    				  java.io.DataOutputStream classes 
				  in the Java Standard Library.)
    <byte>    := (8 bit byte)
    <int16>   := (16 bit signed integer, "short" in Java)
    <int32>   := (32 bit signed integer, "int" in Java)
    <int64>   := (64 bit signed integer, "long" in Java)
    <float32> := (32 bit IEEE floating point value, "float" in Java)
    <float64> := (64 bit IEEE floating point value, "double" in Java)
    <uint32>  := (Google's protocol buffers variable length unsigned 32 bit integer)
    <uint64>  := (Google's protocol buffers variable length unsigned 64 bit integer)
    <sint32>  := (Google's protocol buffers variable length signed 32 bit integer)
    <sint64>  := (Google's protocol buffers variable length signed 64 bit integer)

* A literal byte sequence.  These are generally used as markers and are represented as text in double quotes (`"`).
  These markers will always be constrained to printable ASCII characters and should be encoded as ASCII, one byte per character.
* A literal byte, specified as a hexidecimal string (for example, 0xFF).
* any of the above suffixed by a modifier:

  * A plus sign (`+`) to indicate one or more repetitions
  * An asterisk (`*`) to indicate 0 or more repetitions
  * A number in brackets (`[]`) to indicate a specific number of repetitions.
* Comments sometimes appear to clarify the intent of certain structures.
  These will be enclosed in parentheses (`()`).
 
Geometry
--------

.. code-block:: none

    geometry            := geometryHeader (when an empty geometry) | 
                           geometryHeader geometryBody (when not an empty geometry) 
    geometryHeader      := typeAndMask | typeAndMask precisionModel+
    typeAndMask         := <byte> (made of typeMask & emptyMask & numDimensionsMask)
    dimensionHeader     := 
    geometryBody        := point|lineString|polygon|compoundGeometry|geometryCollection
    point               := coordinateArray
    coordinateSequence  := <uint32> (size) ordinateArray+
    coordinateArray     := 
    ordinateArray: fixedPrecisionArray | singlePrecisionArray | doublePrecisionArray
    fixedPrecisionArray: <sint64>[]
