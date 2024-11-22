import Pyro5.api
import time

def inicia_publicador():
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    print("Publicador conectado ao líder")
    publicacoes = 0
    while True:
        try:
            publicacoes = publicacoes + 1
            msg = f"Publicacao N:{publicacoes}"
            lider.recebe_publicacao("topico", "particao", msg)
            print(f"Enviando publicacao N:{publicacoes}")
        except ValueError :
            print(ValueError)
        time.sleep(1)

if __name__ == "__main__":
    inicia_publicador()