import Pyro5.api
from Broker import Broker

def iniciar_votante():
    daemon = Pyro5.api.Daemon()
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    broker = Broker(lider, uri_lider, "votante", 3)
    uri = daemon.register(broker)
    broker.registraUri(uri)
    print("Votante conectado ao líder")
    print(f"Votante URI registrada: {uri}")
    daemon.requestLoop()


if __name__ == "__main__":
    iniciar_votante()
