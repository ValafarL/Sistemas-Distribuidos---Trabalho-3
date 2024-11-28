import Pyro5.api
import time
import threading


@Pyro5.api.expose
class Broker:
    def __init__(self, lider, uri_lider, estado, id):
        self.liderURI = uri_lider
        self.offset = 0
        self.epoca = "Lider_Epoca1"
        self.participantes = 0
        self.id = id
        self.estado = estado
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
        print(self.estado,' registrado')
        self.lider.registra_broker(self.uri, self.estado, self.id)

    def att_participantes(self, p):
        self.participantes = p
        print(f"participantes att para: {self.participantes}")

    def recebe_notificacao(self):
        print("notificacao de novos dados")
        if self.estado == "votante":
            self.requisita_dados()


    def requisita_dados(self):
        proxyLider = Pyro5.api.Proxy(self.liderURI)
        dados_nova_requisicao = proxyLider.requisicao_dados(self.epoca, self.offset, self.uri)
        if(dados_nova_requisicao):
            dados_nova_requisicao = self.lider.requisicao_dados(dados_nova_requisicao["erro"]["epocaMAX"], dados_nova_requisicao["erro"]["offsetMax"], self.uri)

    def recebe_dados(self, dados):
        if(dados):
            self.logs[self.topico][self.particao][self.epoca]["msgs"].append(dados["msgs"])
            self.logs[self.topico][self.particao][self.epoca]["offset"] = dados["offset"]
            self.offset = dados["offset"]
            proxyLider = Pyro5.api.Proxy(self.liderURI)
            proxyLider.recebe_confirmacao({
                "id": self.id,
                "topico":"topico",
                "particao": "particao",
                "epoca":self.epoca,
                "offset":self.offset
            })
            print(f"dados atualizados, confirmacao enviada:")
            return True
        else:
            print("dados vazios")
            return False
    def recebe_dados_observador(self, dados):
        if(dados):
            dados_faltantes = dados['msgs'][self.offset:]
            self.logs[self.topico][self.particao][self.epoca]["offset"] = dados["offset"]
            self.logs[self.topico][self.particao][self.epoca]["msgs"].append(dados_faltantes)
            self.offset = dados["offset"]
            print(f"dados atualizados observador, confirmacao enviada: ")
        else:
            print("dados vazios")

    def heartbeat_monitor(self):
        liderProxy = Pyro5.api.Proxy(self.liderURI)
        while True:
            time.sleep(5)
            print(f"estado no heartbeat: {self.estado}")
            if self.estado == "votante" and self.lider:
                liderProxy.recebe_heartbeat(self.id)
    def muda_estado(self):
        self.estado = "votante"
        print("Observador",self.uri,"\npassou a ser votante")
        liderProxy = Pyro5.api.Proxy(self.liderURI)
        liderProxy.registra_broker(self.uri, self.estado, self.id)
        self.requisita_dados()