import asyncio

class Broker:

    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 8888  
        self.clients = {}

    async def run(self, reader, writer):
        indirizzo = writer.get_extra_info("peername")   # (ip, porta) del client
        print("Connesso: ", indirizzo)

        # leggo tipo e topic una sola volta all'inizio
        comando = (await reader.readline())[:-1].decode().split(":")
        
        if not comando:
            return
        
        tipe = comando[0]
        topic = comando[1]
        
        if tipe in ("pub", "sub"):
            # salviamo il client nel dizionario
            self.clients[indirizzo] = {"tipe": tipe, "topic": topic, "writer": writer, "reader": reader}
            print(f"Client registrato come {tipe} sul topic '{topic}'")

            try:
            # fase di flusso continua (senza il break errato alla fine)
                while True:
                    if tipe == "pub":
                        # se è un pub, resta in ascolto continuo dei suoi messaggi
                        message = comando[2]
                        
                        # inoltra il messaggio a tutti i sub interessati
                        for _, client_info in list(self.clients.items()):
                            if client_info["topic"] == topic and client_info["tipe"] == "sub":
                                client_info["writer"].write((message + "\n").encode())
                                await client_info["writer"].drain()
                        break
                    elif tipe == "sub":
                        # controlla se il subscriber si disconnette
                        controllo = await reader.readline() 
                        if not controllo:
                            break

            except Exception as e:
                print(f"{e}")

            finally:
                # pulizia finale in caso di uscita controllata dal ciclo
                if indirizzo in self.clients:
                    self.clients.pop(indirizzo)
                print("Client disconnesso: ", indirizzo)
                writer.close()
                await writer.wait_closed()

    async def main(self):
        # start_server apre la porta e chiama run per ogni nuovo client
        self.server = await asyncio.start_server(self.run, self.host, self.port)
        print(f"Server in ascolto sulla porta {self.port}")
        async with self.server:
            await self.server.serve_forever()
                            
if __name__ == "__main__":
    b = Broker()
    asyncio.run(b.main())
