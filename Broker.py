import socketserver, asyncio

class Broker:

    clients = {}

    def __init__(self):
        pass

    async def run(self, writer, reader):

        indirizzo = self.writer.get_extra_info("peername")   # (ip, porta) del client
        print("Connesso:", indirizzo)

        while True:
            #leggo i primi 2 valori del comando dai client per sapere se sono sub o pub e il topic
            for _ in range(2):                                               
                tipe = await reader.reduntil(b';').decode()                   
                if tipe == "pub" or tipe == "sub":
                    topic = await reader.reduntil(b';').decode()
                    if indirizzo not in self.__class__.clients:                                                             #se l'indirizzo non è già salvato 
                        self.__class__.clients[indirizzo] = {"tipe":tipe, "topic":topic, "writer": writer, "reader":reader} #salvo i dati di ogni client sul dizionario clients    
            
            if tipe == "pub":                                                                         #se è un client pub
                message = await reader.readuntil(b'\n')                                               #leggo anche il messaggio
                for addr in self.__class__.clients:                                                   #cerco tutti i client sub con lo stesso topic del pub
                    for client_topic in self.__class__.clients[addr]["topic"]:                      
                        if topic == client_topic and self.__class__.clients[addr]["tipe"] == "sub":
                            self.__class__.clients[addr]["writer"].write(message)                     #mando il messaggio sul loro writer