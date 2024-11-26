import Pyro5.api

class Observador:
        def __init__(self):
            self.ID = 4
            self.estado = "observador"
            self.logs = []


def inicia():
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(Observador)
    servidor_nomes = Pyro5.api.locate_ns()
    uri_lider = servidor_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    lider.testando()
    print(lider)
    
if __name__ == "__main__":
    inicia()