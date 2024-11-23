import Pyro5.api
import time
import threading

@Pyro5.api.expose
class Votante:
    def __init__(self, lider):
        self.participantes = 0
        self.lider = lider
        self.ID = 3
        self.estado = "votante"
        self.logs = {"publicacoes":[]}
        thread_publicacao = threading.Thread(target=self.consumir(), args=(10,))
        thread_publicacao.start()
    
    # def consumir(self):
    #    while True:
    #        try:
    #            msg = self.lider.envia_publicacao_consumidor()
    #            if(msg):
    #                self.logs["publicacoes"].append(msg)
    #                print(f"logs atualizados: {self.logs}")
    #            else:
    #                print(f'Sem mensagens novas: {msg}')
    #        except ValueError :
    #            print(ValueError)
    #        time.sleep(1) 
    def att_participantes(self, p):
        self.participantes = p
        print(f"participantes att para: {self.participantes}")
    
    def recebe_notificacao(self):
        print("notificacao de novos dados")

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
