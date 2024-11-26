import Pyro5.api
import Pyro5.nameserver
import threading
import time

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.quorum = 2
        self.offset = 0
        self.participantes = 0
        self.brokers_votantes = []
        self.brokers_observadores = []
        self.topico = 'topico'
        self.particao = 'particao'
        self.epoca = "Lider_Epoca1"
        self.ID = 1
        self.estado = "líder"
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
    
    def registra_broker(self, uri, estado):
        if(estado == 'votante'):
            self.brokers_votantes.append(uri)
            self.participantes = self.participantes + 1
            self.ultimo_heartbeat = {uri: time.time()}
            thread1 = threading.Thread(target=self.atualiza_votantes)
            thread1.start()
            #self.atualiza_votantes()
                    
        elif(estado == 'observador'):
            self.brokers_observadores.append(Pyro5.api.Proxy(uri))
            
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
        self.notifica_votantes()

            
    def envia_publicacao_consumidor(self):
        print(f" consumindo {self.logs[self.topico][self.particao]}")
        if self.logs[self.topico][self.particao]:
            return self.logs[self.topico][self.particao][-1]
    def testando(self):
        print("testando a proxy")

    def atualiza_votantes(self):
        print("participantes att no LIDER")
        for uri in self.brokers_votantes:
            if(uri):
                try:
                    votanteProxy = Pyro5.api.Proxy(uri)
                    votanteProxy.att_participantes(self.participantes)
                    print(f"Participantes att no votante{votanteProxy}")
                except Pyro5.errors.PyroError :
                    print(f"Erro ao att participantes nos votantes {Pyro5.errors.PyroError}")

    def notifica_votantes(self):
        print(self.brokers_votantes)
        for uri in self.brokers_votantes:
            if(uri):
                try:
                    votante = Pyro5.api.Proxy(uri)
                    votante.recebe_notificacao()
                    print(f"Enviando notificacao {votante}")
                except ValueError :
                    print("Erro ao att participantes nos votantes",ValueError)

    def incrementa_offset(self):
        self.offset = self.offset + 1
        self.logs[self.topico][self.particao][self.epoca]["offset"] = self.offset
    
    def requisicao_dados(self, epoca, offset, uri):# terminar de arrumar aqui
        if(self.verifica_requisicao(epoca, offset)):
            return{"dados": 
            { 
                "epoca": self.epoca,
                "offset": self.offset,
                "msgs": self.logs[self.topico][self.particao][self.epoca]["msgs"][offset:self.offset]
            }}
        else:
            return {"erro": {
                "epocaMAX": self.epoca,
                "offsetMAX": self.offset
            }}
        
    def recebe_confirmacao(self, confirmacao):
        topico = confirmacao["topico"]
        particao = confirmacao["particao"]
        epoca = confirmacao["epoca"]
        offset = confirmacao["offset"]
        id_ = confirmacao["id"]
        
        confirmacoes = self.confirmacao[topico][particao][epoca][offset]
        confirmadoAte = self.logs[topico][particao][epoca]["confirmadoAte"]

        if len(confirmacoes) < 2:
            if id_ not in confirmacoes:
                confirmacoes.append(id_)
                print(f"O ID: {id_} confirmou o offset: {offset}")
                if len(confirmacoes) >= 2 and offset - confirmadoAte == 1:
                    confirmadoAte["confirmadoAte"] = offset
                    print(f"Todos os votantes confirmaram o offset: {offset}")
            else:
                print(f"O ID: {id_} já confirmou o offset: {offset}")

        #if len(self.confirmacao[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"]][confirmacao["offset"]]) < 2:
        #    if confirmacao["id"] not in self.confirmacao[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"][confirmacao["offset"]]]:
        #        self.confirmacao[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"]][confirmacao["offset"]].append(confirmacao["id"])
        #        if (len(self.confirmacao[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"]]) >= 2
        #        and confirmacao["offset"] - 
        #        self.logs[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"]]["confirmadoAte"]
        #        == 1):
        #            self.logs[confirmacao["topico"]][confirmacao["particao"]][confirmacao["epoca"]]["confirmadoAte"] = confirmacao["offset"]
        #        print()
        #    else:
        #        print(f"O ID: {confirmacao["id"]} ja confirmour o offset:{confirmacao["offset"]}")

    def verifica_requisicao(self,epoca, offset):
        if epoca in self.logs[self.topico][self.particao] and offset <= self.logs[self.topico][self.particao][epoca]["offset"]:
            print("epoca e offset estão de acordo com os logs")
            return True
        return False
    def envia_mensagem_votante(self,offset):
        print()

            
    def recebe_heartbeat(self, uri):
        if(uri in self.ultimo_heartbeat):
            self.ultimo_heartbeat[uri] = time.time()
            print(f"heartbeat recebido de {uri}")


    def verifica_heartbeats(self):
        while True:
            time.sleep(1)
            tempo_atual = time.time()
            for uri in self.brokers_votantes:
                if uri in self.ultimo_heartbeat:
                    if (tempo_atual - self.ultimo_heartbeat[uri] > 15):
                        print("tempo atual",tempo_atual)
                        print("tempo ultimo heartbeat",self.ultimo_heartbeat[uri])
                        print(tempo_atual - self.ultimo_heartbeat[uri])
                        #self.brokers_votantes.remove(uri)
                        thread_verifica_heartbeats = threading.Thread(target=self.remove_votante, args=uri)
                        thread_verifica_heartbeats.start()
                        #print(f"Votante {uri} removido por inatividade")
                        #self.atualiza_votantes()
                        print("Votantes atualizados")
    def remove_votante(self, uri):
        print(f"Votante {uri} removido por inatividade")
        self.brokers_votantes.remove(uri)
        self.ultimo_heartbeat.pop(uri)
        self.participantes -= 1
        self.atualiza_votantes()

    def confere_quorum(self):
        if self.participantes >= self.quorum:
            return True
        return False
    
    
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
