import asyncio,sys

class Client:

    def __init__(self):
        pass

    async def invia_o_leggi(self, reader, writer):

        if len(sys.argv) > 2:
            tipe = sys.argv[1]
            topic = sys.argv[2]
            comando = tipe + ":" + topic + "\n"
            if tipe == "pub":
                message = sys.argv[3]
                comando = tipe + ":" + topic + ":" + message + "\n"
            writer.write(comando.encode())
            await writer.drain()
            if tipe == "sub":
                while True:
                    message = await reader.readline()
                        
                    if not message:
                        print("Connessione persa")
                        break

                    print(message.decode())
            writer.close()
            await writer.wait_closed()
    async def main(self):
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", 8888)
            await self.invia_o_leggi(reader, writer)
        except ConnectionRefusedError:
            print("Impossibile connettersi al server")
if __name__ == "__main__":
    p = Client()
    try:
        asyncio.run(p.main())
    except KeyboardInterrupt:
        print("\nClient interrotto")