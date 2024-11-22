import Pyro5.api
import Pyro5.nameserver

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.participantes = 1
        self.brokers_votantes = []
        self.brokers_observadores = []
        self.topico = 'topico'
        self.particao = 'particao'
        self.ID = 1
        self.estado = "líder"
        self.logs = {self.topico: {self.particao: []},
                     "outros":[]
                     }
        self.brokers = {}
    
    def registra_broker(self, uri, estado):
        if(estado == 'votante'):
            self.brokers_votantes.append(uri)
            self.participantes = self.participantes + 1
            print("participantes att no LIDER")
            for votante in self.brokers_votantes:
                if(votante):
                    try:
                        votante = Pyro5.api.Proxy(uri)
                        votante.att_participantes(self.participantes)
                        print(f"Participantes att no votante{votante}")
                    except ValueError :
                        print("Erro ao att participantes nos votantes",ValueError)
                    
        elif(estado == 'observador'):
            self.brokers_observadores.append(uri)
            
    def recebe_publicacao(self, topico, particao, mensagem):
        if topico not in self.logs:
            print("Topico inexistente")
        if particao not in self.logs[topico]:
            print("Particicao inexistente")
        self.logs[self.topico][self.particao].append(mensagem)
        print(f"publicacoes atualizadas: {self.logs[self.topico][self.particao]}")
        
    def envia_publicacao_consumidor(self):
        print(f" consumindo {self.logs[self.topico][self.particao]}")
        if self.logs[self.topico][self.particao]:
            return self.logs[self.topico][self.particao][-1]
    def testando(self):
        print("testando a proxy")

def iniciar_lider():
    daemon = Pyro5.api.Daemon()
    lider = Lider()
    uri = daemon.register(lider)
    Pyro5.api.locate_ns().register("Lider_Epoca1", uri)
    print(f"Líder registrado no serviço de nomes com URI: {uri}")
    print("Líder ativo. Aguardando conexões...")
    daemon.requestLoop()


if __name__ == "__main__":
    iniciar_lider()
