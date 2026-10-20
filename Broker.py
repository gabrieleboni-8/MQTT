import socketserver, asyncio

class Broker:

    def __init__(self):
        self.host = "127.0.0.1"
        self.port = "8888"
        self.clients = {}

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
                    if indirizzo not in self.clients:                                                             #se l'indirizzo non è già salvato 
                        self.clients[indirizzo] = {"tipe":tipe, "topic":topic, "writer": writer, "reader":reader} #salvo i dati di ogni client sul dizionario clients    
            
            if tipe == "pub":                                                                         #se è un client pub
                message = await reader.readuntil(b'\n')                                               #leggo anche il messaggio
                for addr in self.clients:                                                   #cerco tutti i client sub con lo stesso topic del pub
                    for client_topic in self.clients[addr]["topic"]:                      
                        if topic == client_topic and self.clients[addr]["tipe"] == "sub":
                            self.clients[addr]["writer"].write(message)                     #mando il messaggio sul loro writer
                            await self.clients[addr]["writer"].drain()
            break

        self.clients.pop(indirizzo)   #cancello l'indirizzo del client in caso di disconnessione
        print("Client disconnesso: ", indirizzo)
        writer.close()
        await writer.wait_closed()

    async def main(self):
        # start_server apre la porta e chiama run per ogni nuovo client
        self.server = await asyncio.start_server(self.run, self.host, self.port)
        print("Server in ascolto sulla porta 8888")
        async with self.server:
            await self.server.serve_forever()
                            
if __name__ == "__main__":
    try:
        b = Broker()
        asyncio.run(b.main())
    except KeyboardInterrupt:
        b.server.close()