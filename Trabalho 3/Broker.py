import Pyro5.api
import time
import threading


@Pyro5.api.expose
class Votante:
    def __init__(self, lider, uri_lider):
        self.liderURI = uri_lider
        self.offset = 0
        self.epoca = "Lider_Epoca1"
        self.participantes = 0
        self.ID = 2
        self.estado = "votante"
        self.logs = {self.epoca: []}
        self.topico = "topico"
        self.particao = "particao"
        self.logs = {self.topico: {self.particao: {self.epoca: {
            "offset": self.offset,
            "msgs": []
        }}}
                     }
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
        self.requisita_dados()

    def incrementa_offset(self):
        self.offset = self.offset + 1  

    def requisita_dados(self):
        self.incrementa_offset
        time.sleep(10)
        proxyLider = Pyro5.api.Proxy(self.liderURI)
        erro = proxyLider.requisicao_dados(self.epoca, self.offset, self.uri)
        if(erro):
            erro = self.lider.requisicao_dados(erro["erro"]["epocaMAX"], erro["erro"]["offsetMax"], self.uri)

    def recebe_dados(self, dados):
        if(dados):
            self.logs[self.topico][self.particao][self.epoca]["offset"] = dados["offset"]
            self.logs[self.topico][self.particao][self.epoca]["msgs"].append(["msgs"])
            self.offset = dados["offset"]
            proxyLider = Pyro5.api.Proxy(self.liderURI)
            proxyLider.recebe_confirmacao({
                "id":self.ID,
                "topico":"topico",
                "particao": "particao",
                "epoca":self.epoca,
                "offset":self.offset
            })
            print(f"dados recebidos, confirmacao enviada: {dados}")
            return True
        else:
            print("dados vazios")
            return False
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
