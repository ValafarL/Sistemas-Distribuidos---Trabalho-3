import Pyro5.api
import Pyro5.nameserver
import threading
import time

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.quorum = 2
        self.offset = 0
        self.participantes = 1
        self.brokers_votantes = []
        self.brokers_observadores = []
        self.topico = 'topico'
        self.particao = 'particao'
        self.epoca = "Lider_Epoca1"
        self.id = 1
        self.estado = "lider"
        self.logs = {self.topico: {self.particao: {self.epoca: {
            "offset": self.offset,
            "confirmadoAte": 0,
            "msgs": []
        }}}}
        self.confirmacao = {self.topico: {self.particao: {self.epoca: [
            []] # armazena os IDs dos confirmados em um array para cada offset, o primeiro array é acessado usando offset como index 
        }}}
        self.brokers = {}
        self.ultimo_heartbeat = {}
        self.heartbeat_timer = 0
        thread_verifica_heartbeats = threading.Thread(target=self.verifica_heartbeats)
        thread_verifica_heartbeats.start()
        thread_quorum = threading.Thread(target=self.confere_quorum)
        thread_quorum.start()
        thread_escuta_publicacao = threading.Thread(target=self.escuta_publicacao)
        thread_escuta_publicacao.start()
    
    #requisito 1
    def ler_mensagem(self, topico, particao, epoca, offset):
        confirmadas = self.logs[topico][particao][epoca]['confirmadoAte']
        return self.logs[topico][particao][epoca]["msgs"][offset:confirmadas]
    
    #requisito 1
    def escuta_publicacao(self):
        offsetAnt = 0
        while True:
            if offsetAnt < self.offset:
                try:
                    thread_notifica = threading.Thread(target=self.notifica_votantes)
                    thread_notifica.start()
                    offsetAnt = self.offset
                except ConnectionRefusedError:
                    print('Erro ao conectar ao votante')

    #requisito 1
    def registra_broker(self, uri, estado, id):
        if(estado == 'votante'):
            self.brokers_votantes.append({"uri": uri, "id": id})
            self.participantes = self.participantes + 1
            self.ultimo_heartbeat[id] = time.time()
            thread1 = threading.Thread(target=self.atualiza_votantes)
            thread1.start()
            #self.atualiza_votantes()
                    
        elif(estado == 'observador'):
            self.brokers_observadores.append(uri)
    
    #requisito 2.1
    def recebe_publicacao(self, topico, particao, mensagem):
        if topico not in self.logs:
            print("Topico inexistente")
        if particao not in self.logs[topico]:
            print("Particicao inexistente")
        self.logs[self.topico][self.particao][self.epoca]["msgs"].append(mensagem)
        self.confirmacao[self.topico][self.particao][self.epoca].append([])
        print(f"publicacoes atualizadas:")
        self.incrementa_offset()
        print("offset",self.offset)
        #self.notifica_votantes()

    #requisito 1
    def envia_publicacao_consumidor(self):
        print(f" consumindo {self.logs[self.topico][self.particao]}")
        if self.logs[self.topico][self.particao]:
            return self.logs[self.topico][self.particao][-1]

    #requisito 1, 3.
    def atualiza_votantes(self):
        print("participantes att no LIDER")
        for votante in self.brokers_votantes:
            if(votante):
                try:
                    votanteProxy = Pyro5.api.Proxy(votante["uri"])
                    votanteProxy.att_participantes(self.participantes)
                    print(f"Participantes att no votante{votanteProxy}")
                except Pyro5.errors.PyroError :
                    print(f"Erro ao att participantes nos votantes {Pyro5.errors.PyroError}")
                    print(votante['id'])
    #requisito 2.2
    def notifica_votantes(self):
        print(self.brokers_votantes)
        for votante in self.brokers_votantes:
            if(votante):
                try:
                    votante = Pyro5.api.Proxy(votante["uri"])
                    votante.recebe_notificacao()
                    print(f"Enviando notificacao ")
                except ConnectionRefusedError :
                    print(f"Conexao perdido com o votante: {votante['id']}")
    #requisito 2.1
    def incrementa_offset(self):
        self.offset = self.offset + 1
        self.logs[self.topico][self.particao][self.epoca]["offset"] = self.offset
    
    #requisito 2.4
    def requisicao_dados(self, epoca, offset, uri):
        if not self.verifica_requisicao(epoca, offset):
            return {"erro": {
                "epocaMAX": self.epoca,
                "offsetMAX": self.offset
            }}
        proxyV = Pyro5.api.Proxy(uri)
        proxyV.recebe_dados({ 
                "epoca": self.epoca,
                "topico": self.topico,
                "particao": self.particao,
                "offset": self.offset,
                "msgs": self.logs[self.topico][self.particao][self.epoca]["msgs"][offset:self.offset]
            })
        return None
    #requisito 2.4
    def verifica_requisicao(self,epoca, offset):
        if epoca in self.logs[self.topico][self.particao] and offset <= self.logs[self.topico][self.particao][epoca]["offset"]:
            print("epoca e offset estão de acordo com os logs")
            return True
        return False
    #requisito 2.6, 2.7
    def recebe_confirmacao(self, confirmacao):
        topico = confirmacao["topico"]
        particao = confirmacao["particao"]
        epoca = confirmacao["epoca"]
        offset = confirmacao["offset"]
        id_ = confirmacao["id"]
        
        confirmacoes = self.confirmacao[topico][particao][epoca][offset]
        confirmadoAte = self.logs[topico][particao][epoca]["confirmadoAte"]
        if id_ not in confirmacoes:
            self.confirmacao[topico][particao][epoca][offset].append(id_)
            print(f"O ID: {id_} confirmou o offset: {offset}")
            if len(confirmacoes) >= self.participantes//2 + 1 and offset - confirmadoAte == 1:
                self.logs[topico][particao][epoca]["confirmadoAte"] = offset
                print(f"Todos os votantes confirmaram o offset: {offset}")
                self.envia_confirmacao_votantes(offset)
        else:
            print(f"O ID: {id_} já confirmou o offset: {offset}")
    def envia_confirmacao_votantes(self, offset):
        print("enviando confrimacao para todos os votantes")
        for votante in self.brokers_votantes:
            if(votante):
                try:
                    votante = Pyro5.api.Proxy(votante["uri"])
                    votante.recebe_confirmacao({
                        "topico":"topico",
                        "particao": "particao",
                        "epoca":self.epoca,
                        "offset":self.offset,
                        "id_confirmados": self.confirmacao['topico']['particao'][self.epoca][offset]
                    })
                except ConnectionRefusedError :
                    print(f"Conexao perdido com o votante: {votante['id']}")
    #requisito 3.4
    def envia_dados_observador(self):
        print("enviando dados para o observador")
        confirmado = self.logs[self.topico][self.particao][self.epoca]['confirmadoAte']
        for uri in self.brokers_observadores:
            if uri:
                print('DENTRO DO IF')
                proxyO = Pyro5.api.Proxy(uri)
                proxyO.recebe_dados_observador({ 
                    "epoca": self.epoca,
                    "offset": self.offset,
                    "msgs": self.logs[self.topico][self.particao][self.epoca]["msgs"][0:confirmado]
                })
    #requisito 3.1
    def recebe_heartbeat(self, id):
        if(id in self.ultimo_heartbeat):
            self.ultimo_heartbeat[id] = time.time()
            print(f"heartbeat recebido de {id}")

    #requisito 3.2
    def verifica_heartbeats(self):
        while True:
            time.sleep(1)
            tempo_atual = time.time()
            for votante in self.brokers_votantes:
                if votante["id"] in self.ultimo_heartbeat:
                    if (tempo_atual - self.ultimo_heartbeat[votante["id"]] > 15):
                        #print("tempo atual",tempo_atual)
                        #print("tempo ultimo heartbeat",self.ultimo_heartbeat[votante["id"]])
                        #print(tempo_atual - self.ultimo_heartbeat[votante["id"]])
                        #print(votante)
                        thread_verifica_heartbeats = threading.Thread(target=self.remove_votante, args=[votante["id"]])
                        thread_verifica_heartbeats.start()
                        print("Votantes atualizados")
    #requisito 3.2
    def remove_votante(self, id):
        for votante in self.brokers_votantes:
            if votante['id'] == id:
                self.brokers_votantes.remove(votante)
                print(f"Votante com ID {id} removido")
                break
        print("broker att apos remover: ", self.brokers_votantes)
        self.ultimo_heartbeat.pop(id)
        self.participantes = self.participantes - 1
        self.atualiza_votantes()
    #requisito 3.3
    def confere_quorum(self):
        while True:
            time.sleep(1)
            if self.verifica_quorum() and self.brokers_observadores:
                print("entrou")
                observadorURI = self.brokers_observadores.pop(0)
                proxyO = Pyro5.api.Proxy(observadorURI)
                proxyO.muda_estado()
                
    #requisito 3.3
    def verifica_quorum(self):
        return self.participantes < self.quorum
    
    
def iniciar_lider():
    daemon = Pyro5.api.Daemon()
    lider = Lider()
    uri = daemon.register(lider)
    Pyro5.api.locate_ns().register("Lider_Epoca1", uri)
    print(f"Líder registrado no serviço de nomes com URI: {uri}")
    print("Líder ativo. Aguardando conexões...")
    daemon.requestLoop()


if __name__ == "__main__":
    iniciar_lider()
