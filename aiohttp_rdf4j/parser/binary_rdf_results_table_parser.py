import logging
import struct
import collections
from rdflib import BNode, Literal, URIRef

logger = logging.getLogger(__name__)

# MAGIC NUMBER
MAGIC_NUMBER =  b'BRTR'

FORMAT_VERSION = 4
# RECORD TYPES 
NULL_RECORD_MARKER = 0
REPEAT_RECORD_MARKER = 1
NAMESPACE_RECORD_MARKER = 2
QNAME_RECORD_MARKER = 3
URI_RECORD_MARKER = 4
BNODE_RECORD_MARKER = 5
PLAIN_LITERAL_RECORD_MARKER = 6
LANG_LITERAL_RECORD_MARKER = 7
DATATYPE_LITERAL_RECORD_MARKER = 8
EMPTY_ROW_RECORD_MARKER = 9
ERROR_RECORD_MARKER = 126
TABLE_END_RECORD_MARKER = 127
# ERROR TYPES 
MALFORMED_QUERY_ERROR = 1
QUERY_EVALUATION_ERROR = 2


class BinaryQueryResultParser:
    #FIXME Better exceptions!


    def __init__(self, stream):
        self.stream = stream
        #self._resp = resp
        self.namespaceDict = {}
        self.buffer =b""



    async def __aiter__(self):
        return self.parse()


    @property
    def vars(self):
        try:
            return self._vars
        except AttributeError as why:
            logger.exception(f"{self} not propery initilazed. You need to call await self._init(). Error: {why}")


    async def _init(self):
        if not hasattr(self, "_vars"):
            mn = await self.read_(4)
            if mn != MAGIC_NUMBER:
                ##print ("No Magic")
                raise IOError(f"{mn}: Bad Magic Number")
                # return
            self.formatVersion, = await self.read_(4, "!i")
            if self.formatVersion > FORMAT_VERSION and self.formatVersion < 1:
                raise IOError(f"{fv}: Wrong Version")
                # return
            if self.formatVersion == 2:
                await self.read(4, "!i")

            # self.formatVersion, mn)
            # Read column headers
            columnCount, = await self.read_(4, "!i")
            # #print(columnCount)
            if columnCount < 0:
                raise IOError(f"Illegal column count specified: {columnCount}")

            columnHeaders = []

            for _ in range(columnCount):
                columnHeaders.append(await self.readString())
            self._vars = tuple(columnHeaders)



    async def read_(self, n, t=None):
        while len(self.buffer) < n:
            chunk, eos = await self.stream.readchunk()
            self.buffer += chunk
        res, self.buffer = self.buffer[:n], self.buffer[n:]
        return struct.unpack(t, res) if t else res


    async def parse(self):

        class ResultRow(collections.namedtuple("ResultRow", self._vars)):
            __slots__ = ()
            def __getitem__(self, name):
                if isinstance(name, int) and name < len(self._fields):
                    return super().__getitem__(name)
                elif isinstance(name, str) and name in self._fields:
                    return getattr(self, name)
                else:
                    raise KeyError(name)

        #ResultRow.__getitem__ = getitem_overwrite
        previousTuple = []
        values = []
        #recordTypeMarker,  = await self.read_(1, "!b")
        while True:
            recordTypeMarker, = await self.read_(1, "!b")
            ##print("rtm", recordTypeMarker)
            if recordTypeMarker == TABLE_END_RECORD_MARKER:
                break
            elif recordTypeMarker == ERROR_RECORD_MARKER:
                await self.processError()
            elif recordTypeMarker == NAMESPACE_RECORD_MARKER:
                ##print("NS R")
                await self.processNamespace()
            elif recordTypeMarker == EMPTY_ROW_RECORD_MARKER:
                pass
            else:
                if recordTypeMarker==NULL_RECORD_MARKER:
                    values.append(None)
                    #pass
                elif recordTypeMarker==REPEAT_RECORD_MARKER:
                    values.append(previousTuple[len(values)])
                elif recordTypeMarker==QNAME_RECORD_MARKER:
                    values.append(await self.readQName())
                elif recordTypeMarker==URI_RECORD_MARKER:
                    values.append(await self.readURI())
                elif recordTypeMarker==BNODE_RECORD_MARKER:
                    values.append(await self.readBnode())
                elif recordTypeMarker in (PLAIN_LITERAL_RECORD_MARKER, LANG_LITERAL_RECORD_MARKER, DATATYPE_LITERAL_RECORD_MARKER):
                    values.append(await self.readLiteral(recordTypeMarker))
                else:
                    raise IOError("Unkown record type: " + str(recordTypeMarker));
            ###print("values",values)
            if len(values) == len(self._vars):
                yield ResultRow._make(values)
                previousTuple = values
                values = []
            #recordTypeMarker,  = await self.read_(1, "!b")


    async def processError(self):
        errTypeFlag = await self.read_(1, "!b")
        errType = "";
        if errTypeFlag == MALFORMED_QUERY_ERROR:
            errType = "MALFORMED_QUERY_ERROR"

        elif errTypeFlag == QUERY_EVALUATION_ERROR:
            errType = "QUERY_EVALUATION_ERROR"

        else:
            raise IOError("Unkown error type: " + errTypeFlag)

        msg = self.readString();
        # FIXME: is this the right thing to do upon encountering an error?
        raise IOError(errType + ": " + msg);


    async def processNamespace(self):

        namespaceID, = await self.read_(4,"!i")
        namespace = await self.readString()
        ##print("NS", namespaceID, namespace)
        self.namespaceDict[namespaceID] = namespace


    async def readQName(self):
        #print(self.namespaceDict)
        nsID, = await self.read_(4,"!i")
        localName = await self.readString()
        uri = self.namespaceDict[nsID]+localName
        return URIRef(uri)


    async def readURI(self):
        return URIRef(await self.readString())


    async def readBnode(self):
        return BNode(await self.readString())

    async def readLiteral(self, recordTypeMarker):
        label = await self.readString()
        if recordTypeMarker == DATATYPE_LITERAL_RECORD_MARKER:
            datatype = None
            dtTypeMarker,  = await self.read_(1, "!b")
            if dtTypeMarker == QNAME_RECORD_MARKER:
                datatype = await self.readQName()

            elif dtTypeMarker == URI_RECORD_MARKER:
                datatype = await self.readURI()
            else:
                raise IOError("Illegal record type marker for literal's datatype")
            return Literal(label, datatype=datatype)
        elif recordTypeMarker == LANG_LITERAL_RECORD_MARKER:
            language = await self.readString()
            return Literal(label, lang=language)
        else:
            return Literal(label)



    async def readString(self):
        if self.formatVersion == 1:
            raise IOError("Version 1 not supported yet")
        else:
            return await self.readStringV2()

	#/**
	# * Reads a string from the version 1 format, i.e. in Java's {@link DataInput#modified-utf-8 Modified
	# * UTF-8}.
	# */
    async def readStringV1(self):
        raise NotImplementedError

    async def readStringV2(self):
        length, = await self.read_(4, "!i")
        stringBytes = length
        _t  = await self.read_(stringBytes)
        string = _t.decode("utf-8")
        return string
