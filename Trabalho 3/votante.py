import Pyro5.api
import time
import threading


@Pyro5.api.expose
class Votante:
    def __init__(self, lider):
        self.participantes = 0
        self.ID = 2
        self.estado = "votante"
        self.logs = []
        self.publicacoes = 0
        self.lider = lider
        thread_publicacao = threading.Thread(target=self.publicar(), args=(10,))
        thread_publicacao.start()
        
    def att_participantes(self, p):
        self.participantes = p
        print(f"participantes att para: {self.participantes}")


def iniciar_votante():
    daemon = Pyro5.api.Daemon()
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    votante = Votante(lider)
    uri = daemon.register(votante)
    print("Votante conectado ao líder")
    print(f"Votante URI registrada: {uri}")
    lider.testando()
    daemon.requestLoop()


if __name__ == "__main__":
    iniciar_votante()
