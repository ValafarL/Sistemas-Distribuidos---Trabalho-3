import Pyro5.api
import time

def inicia_consumidor():
    servico_nomes = Pyro5.api.locate_ns()
    uri_lider = servico_nomes.lookup("Lider_Epoca1")
    lider = Pyro5.api.Proxy(uri_lider)
    print("consumidor conectado ao líder")
    while True:
        time.sleep(3)
        try:
            print("Insira o offset da mensagem desejada: ")
            offset = int(input())
            msgs = lider.ler_mensagem("topico", "particao", "Lider_Epoca1", offset)
            print(f"mensagens recebidas: {msgs} ")
        except ValueError :
            print(ValueError)

if __name__ == "__main__":
    inicia_consumidor()