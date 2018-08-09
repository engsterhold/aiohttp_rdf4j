import logging
import struct
from rdflib import BNode, Literal, URIRef
from aiohttp_rdf4j.utils import Statement

logger = logging.getLogger(__name__)

# Constants
MAGIC_NUMBER = b'BRDF'

FORMAT_VERSION = 1

#record types

NAMESPACE_DECL = 0
STATEMENT = 1
COMMENT = 2
VALUE_DECL = 3
ERROR = 126
END_OF_DATA = 127

#value types

NULL_VALUE = 0
URI_VALUE = 1
BNODE_VALUE = 2
PLAIN_LITERAL_VALUE = 3
LANG_LITERAL_VALUE = 4
DATATYPE_LITERAL_VALUE = 5
VALUE_REF = 6



class BinaryRDFParser:

    def __init__(self, stream):
        self.declaredValues = dict()
        self.namespaces = dict()
        #Utf8Decoder = codecs.getincrementaldecoder('utf-16be')
        #self.decoder = Utf8Decoder()
        self.stream = stream
        self.buffer = b""
        #self.vars = None #Dummy attribute so that query can ask for vars in construct
        #self.cache = collections.deque()


    async def read_(self,n, t=None):
        while len(self.buffer)<n:
            chunk, eos = await self.stream.readchunk()
            self.buffer+=chunk
        res, self.buffer = self.buffer[:n], self.buffer[n:]
        return struct.unpack(t, res) if t else res



    async def parse(self):
        mn= await self.read_(4)
        if mn != MAGIC_NUMBER:
            ##print ("No Magic")
            raise IOError("Bad Magic Number")
            #return

        fv, = await self.read_(4,"!i")
        #print("fv", fv)
        if  fv != FORMAT_VERSION:
            raise IOError("Wrong Version")
            #return

        while True:
            recordType, = await self.read_(1,"!b")
            #print("in while", recordType)
            if recordType == END_OF_DATA:
                break
            elif recordType == STATEMENT:
                yield await self.readStatement()

            elif recordType == VALUE_DECL:
                await self.readValueDecl()
            elif recordType == NAMESPACE_DECL:
                #print("ns")
                await self.readNamespaceDecl()
            elif recordType == COMMENT:
                await self.readComment()
            else:
                raise IOError("Parse Error for recordType %s", recordType)
                break




    async def readNamespaceDecl(self):
        #print("in namespace")
        prefix = await self.readString()
        #print("prefix", prefix)
        namespace = await self.readString()
        #print("namespace", namespace)
        self.namespaces[prefix] = namespace

    async def readComment(self):
        comment = await self.readString()

    async def readValueDecl(self):
        #self.stream_caching(4)
        id, = await self.read_(4, "!i")
        v = await self.readValue()
        self.declaredValues[id] = v

    async def readStatement(self):

        subj = await self.readValue()
        #print("subj", subj)
        pred = await self.readValue()
        #print("pred", pred)
        obj = await self.readValue()
        #print("obj", obj)
        ctx = await self.readValue()
        #print("ctx", ctx)

        #print ("ii", subj,pred,obj, ctx)
        return Statement(subj,pred,obj,ctx)

        #return ((subj, pred, obj), ctx)




    async def readValue(self):

        valueType, = await self.read_(1, "!b")
        ##print(valueType)
        if valueType == NULL_VALUE:
            return None
        elif valueType == VALUE_REF:
            return await self.readValueRef()
        elif valueType == URI_VALUE:
            return await self.readUri()
        elif valueType == BNODE_VALUE:
            return await self.readBNode()
        elif valueType == PLAIN_LITERAL_VALUE:
            return await self.readPlainLiteral()
        elif valueType == LANG_LITERAL_VALUE:
            return await self.readLangLiteral()
        elif valueType == DATATYPE_LITERAL_VALUE:
            return await self.readDatatypeLiteral()
        else:
            return None

    async def readValueRef(self):
        id, = await self.read_(4, "!i")
        return self.declaredValues[id]

    async def readUri(self):
        return URIRef(await self.readString())

    async def readBNode(self):
        return BNode(await self.readString())

    async def readPlainLiteral(self):
        return Literal(await self.readString())


    async def readLangLiteral(self):
        label = await self.readString()
        language = await self.readString()
        return Literal(label, lang=language)



    async def readDatatypeLiteral(self):
        label = await self.readString()
        datatype = await self.readString()
        dt = URIRef(datatype)
        return Literal(label, datatype=dt)


    async def readString(self):
        ##print(x)
        length, = await self.read_(4, "!i")
        stringBytes = length << 1
        ##print(self.stream.getbuffer().nbytes, stringBytes, self.stream.tell())
        #self.stream_caching(length)
        _t = await self.read_(stringBytes)
        string = _t.decode("utf-16be")
        #string = self.decoder.decode(_t)
        ##print (string)
        return string


    async def __aiter__(self):
        return self.parse()

