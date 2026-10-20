import socketserver, asyncio

class Broker:

    clients = {}

    def __init__(self):
        pass

    async def run(self, writer, reader):

        indirizzo = self.writer.get_extra_info("peername")   # (ip, porta) del client
        print("Connesso: ", indirizzo)

        while True:
            #leggo i primi 2 valori del comando dai client per sapere se sono sub o pub e il topic
            for _ in range(2):                                               
                tipe = await reader.reduntil(b':').decode()
                
                if tipe == None:
                    break

                if tipe == "pub" or tipe == "sub":
                    topic = await reader.reduntil(b':').decode()
                    if indirizzo not in self.__class__.clients:                                                             #se l'indirizzo non è già salvato 
                        self.__class__.clients[indirizzo] = {"tipe":tipe, "topic":topic, "writer": writer, "reader":reader} #salvo i dati di ogni client sul dizionario clients    
            
            if tipe == "pub":                                                                         #se è un client pub
                message = await reader.readuntil(b'\n')                                               #leggo anche il messaggio
                for addr in self.__class__.clients:                                                   #cerco tutti i client sub con lo stesso topic del pub
                    for client_topic in self.__class__.clients[addr]["topic"]:                      
                        if topic == client_topic and self.__class__.clients[addr]["tipe"] == "sub":
                            self.__class__.clients[addr]["writer"].write(message)                     #mando il messaggio sul loro writer
                            await self.__class__.clients[addr]["writer"].drain()
            break

        self.__class__.clients.pop(indirizzo)   #cancello l'indirizzo del client in caso di disconnessione
        print("Client disconnesso: ", indirizzo)
        writer.close()
        await writer.wait_closed()

    async def main():
        # start_server apre la porta e chiama run per ogni nuovo client
        server = await asyncio.start_server(__class__.run, "", 8888)
        print("Server in ascolto sulla porta 8888")
        async with server:
            await server.serve_forever()
                            
if __name__ == "__main__":
    b = Broker()
    asyncio.run(b.main())