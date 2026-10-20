import socketserver, asyncio

class Publisher:

    def __init__(self):
        pass

    async def invia(self, writer):
        while True:
            testo = await asyncio.to_thread(input("> "))   # input() gira in un thread separato
            if testo == "/quit" or writer.is_closing():
                break
            writer.write((testo + "\n").encode())
            await writer.drain()
        writer.close()

    async def main(self):
        _, writer = await asyncio.open_connection("127.0.0.1", 8888)
        print("Connesso alla chat. Scrivi /quit per uscire.")

        # le due coroutine girano in parallelo
        await asyncio.gather(self.invia(writer))

if __name__ == "__main__":
    p = Publisher()
    asyncio.run(p.main())