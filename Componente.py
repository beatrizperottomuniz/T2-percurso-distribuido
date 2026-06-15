#!/usr/bin/env python3

from pika import BlockingConnection
from sys import argv
from enum import Enum
import time

Estado = Enum('Estado', 'INICIADOR OCIOSO VISITADO OK')

if len(argv) < 2:
    print(f"USO: {argv[0]} <id> [<v1> <v2> ...]")
    exit(1)

idx = argv[1]
Nx = argv[2:]

estado         = Estado.OCIOSO
nao_visitados  = list(Nx)
entrada        = None
iniciador   = False # true se foi ativado pelo starter
tempo_inicio   = None # tempo q token foi enviado pela primeira vez

print("meu id =", idx)
print("meus vizinhos =", Nx)

conexao = BlockingConnection()
canal   = conexao.channel()

# obs : subir todos antes de dar starter pra ja ter as filas
canal.queue_declare(queue=idx, auto_delete=True)

# envia mensagem para dest
def envia(tipo, dest):
    # envia mensagem do tipo T/R/B para um destino
    canal.basic_publish(exchange="", 
                        routing_key=dest, 
                        body=f"{idx}:{tipo}")
    print(f"[{idx}] -> {tipo} -> {dest}")

# do algoritmo
def visita():
    # se tem viz n visitados -> extrai o prox e envia T
    # sn marca OK e devolve R pro pai (ou encerra se iniciador).
    global estado, nao_visitados, tempo_inicio

    time.sleep(1)   # tempo de 1 s para ver a execucao

    if nao_visitados:
        prox = nao_visitados.pop(0)     #  prox vizinho a visitar
        estado = Estado.VISITADO
        print(f"[{idx}] enviando TOKEN para {prox} | resta = {nao_visitados}")
        envia('T', prox)
    else:
        estado = Estado.OK
        if iniciador: # pra mostrar quando acaba
            tempo = time.time() - tempo_inicio
            print(f"--------\ \n[{idx}] - Percurso concluído em {tempo:.1f}s --------\n")
        else:
            print(f"[{idx}] sem mais vizinhos = mandando R pra {entrada}")
            envia('R', entrada)
 

def recebendo(tipo, origem):
    global estado, nao_visitados, entrada, iniciador

    print(f"[{idx}] <-- {tipo} recebido de {origem}")

    # OCIOSO recebe T pela primeira vez
    if estado == Estado.OCIOSO and tipo == 'T':
        entrada = origem
        nao_visitados = [v for v in Nx if v != origem]  # tira quem mandou
        iniciador = False
        visita()

    # VISITADO recebe T (ja foi visitado -> back-edge)
    elif estado == Estado.VISITADO and tipo == 'T':
        # tira origem dos não_visitados - aresta = back-edge
        if origem in nao_visitados:
            nao_visitados.remove(origem)
        print(f"[{idx}] back-edge detectado com {origem}, enviando B")
        envia('B', origem)

    # VISITADO recebe R (sub-arv concluida)
    elif estado == Estado.VISITADO and tipo == 'R':
        visita()

    # VISITADO recebe BACKEDGE confirmado
    elif estado == Estado.VISITADO and tipo == 'B':
        visita()

def espontaneamente(msg):
    global estado, nao_visitados, iniciador, tempo_inicio
    print("iniciador:", msg)
    estado = Estado.INICIADOR
    nao_visitados = list(Nx)
    iniciador = True
    tempo_inicio = time.time()
    visita()

def callback(canal, metodo, props, corpo):
    # Formato esperado das mensagens:
    # "STARTER:<text>"  - evento espontâneo
    # "<ORIGEM>:<TIPO>" - T/R/B de outro no

    m = corpo.decode().split(":")
    if len(m) < 2:
        print(f"[{idx}] mensagem malformada: {corpo.decode()}")
        return
    
    origem = m[0]
    msg = m[1]

    if origem.upper() == 'STARTER':
        espontaneamente(msg)
    else:
        recebendo(msg, origem)

# loop consumo --------------------------
canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)

try:
    print(f"{idx}: aguardando mensagens...")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()
print("Terminando")
