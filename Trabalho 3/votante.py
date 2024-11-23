import Pyro5.api
import time
import threading


@Pyro5.api.expose
class Votante:
    def __init__(self, lider, uri_lider):
        self.liderURI = uri_lider
        self.offSet = 0
        self.epoca = "Lider_Epoca1"
        self.participantes = 0
        self.ID = 2
        self.estado = "votante"
        self.logs = {self.epoca: []}
        self.publicacoes = 0
        self.lider = lider
        thread_heartbeat = threading.Thread(target=self.heartbeat_monitor)
        thread_heartbeat.start()

    def registraUri(self, uri):
        self.uri = uri
        print('votante registrado')
        self.lider.registra_broker(self.uri, 'votante')

    def att_participantes(self, p):
        self.participantes = p
        print(f"participantes att para: {self.participantes}")

    def recebe_notificacao(self):
        print("notificacao de novos dados")
        self.requisicao_dados()

    def incrementa_offset(self):
        self.offset = self.offset + 1  

    def requisicao_dados(self):
        self.incrementa_offset
        resposta = self.lider.requisicao_dados(self.epoca, 0, self.uri)
        if("offsetMax" in  resposta):
            print()
        elif("dados" in resposta):
            print()

    def lider_offset_max(self, offset):
        print('')

    def heartbeat_monitor(self):
        liderProxy = Pyro5.api.Proxy(self.liderURI)
        while self.estado == "votante":
            time.sleep(5)
            if self.lider:
                liderProxy.recebe_heartbeat(self.uri)
    def transforma_estado(self):
        print()

def iniciar_votante():
    daemon = Pyro5.api.Daemon()
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    votante = Votante(lider, uri_lider)
    uri = daemon.register(votante)
    votante.registraUri(uri)
    print("Votante conectado ao líder")
    print(f"Votante URI registrada: {uri}")
    daemon.requestLoop()


if __name__ == "__main__":
    iniciar_votante()
