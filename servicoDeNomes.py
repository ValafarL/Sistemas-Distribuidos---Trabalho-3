import Pyro5.api
import Pyro5.nameserver

def inicia_servico_de_nomes():
    Pyro5.nameserver.start_ns_loop()    
if __name__=='__main__':
    inicia_servico_de_nomes()