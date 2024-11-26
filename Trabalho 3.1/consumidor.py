import Pyro5.api
import time

def inicia_publicador():
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    print("Consumidor conectado ao líder")
    while True:
        try:
            print(f"nao esta consumindo")
        except ValueError :
            print(ValueError)
        time.sleep(1)

if __name__ == "__main__":
    inicia_publicador()