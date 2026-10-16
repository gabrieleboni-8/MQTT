import socketserver, asyncio

class Broker:

    clients = {}

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def run(self):

        indirizzo = self.writer.get_extra_info("peername")   # (ip, porta) del client
        print("Connesso:", indirizzo)

        while True:
            # leggo i primi 2 valori del comando dai client per sapere se sono sub o pub e il topic
            for _ in range(2):                                               
                tipe = await self.reader.reduntil(b';').decode()                   
                if tipe == "pub" or tipe == "sub":
                    topic = await self.reader.reduntil(b';').decode()
                    self.__class__.clients[indirizzo] = {"tipe":tipe, "topic":topic}                   
            
            if self.__class__.clients[indirizzo]["tipe"] == "pub":
                message = await self.reader.readuntil(b'\n')
                