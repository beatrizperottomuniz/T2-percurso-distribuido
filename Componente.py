#!/usr/bin/env python3
from pika import BlockingConnection
from sys import argv
from enum import Enum
import time

if len(argv) < 2:
    print(f"USO: {argv[0]} <id> [<v1> <v2> ...]")
    exit(1)

idx = argv[1]
Nx  = argv[2:]

print(f"[{idx}] iniciando  |  vizinhos: {Nx}")

Estado = Enum('Estado', 'OCIOSO VISITADO OK')

estado         = Estado.OCIOSO
nao_visitados  = list(Nx)   # cópia mutável dos vizinhos
entrada        = None        # nó de quem recebemos T pela primeira vez
eh_iniciador   = False       # True se foi ativado pelo starter
tempo_inicio   = None        # instante em que o token foi enviado pela 1ª vez

# conexao AMQP
conexao = BlockingConnection()
canal   = conexao.channel()

# declara a fila deste nó e as filas dos vizinhos (para poder publicar)
canal.queue_declare(queue=idx, auto_delete=True)
for v in Nx:
    canal.queue_declare(queue=v, auto_delete=True)

# funcs comunicacao
def envia(tipo, dest):
    """Envia mensagem do tipo T/R/B para um único destino (string)."""
    corpo = f"{idx}:{tipo}"
    canal.basic_publish(exchange="", routing_key=dest, body=corpo)
    print(f"[{idx}] --> {tipo} --> {dest}")

# ---------------------------------------------------------------------------
def visita():
    global estado, nao_visitados, tempo_inicio

    time.sleep(1)   # temporização de 1 segundo para visualizar a execução

    if nao_visitados:
        prox = nao_visitados.pop(0)     # extrai o próximo vizinho a visitar
        estado = Estado.VISITADO
        print(f"[{idx}] enviando TOKEN para {prox}  |  restantes: {nao_visitados}")
        envia('T', prox)
    else:
        estado = Estado.OK
        if eh_iniciador:
            elapsed = time.time() - tempo_inicio
            print(f"\n[{idx}] === PERCURSO CONCLUÍDO em {elapsed:.1f}s ===\n")
        else:
            print(f"[{idx}] sem mais vizinhos, devolvendo R para {entrada}")
            envia('R', entrada)

# Handlers dos eventos do algoritmo
def espontaneamente(msg):
    global estado, nao_visitados, eh_iniciador, tempo_inicio

    print(f"[{idx}] *** INICIADOR ativado pelo starter (msg={msg}) ***")
    nao_visitados = list(Nx)   # todos os vizinhos ainda por visitar
    eh_iniciador  = True
    tempo_inicio  = time.time()
    visita()


def recebendo(tipo, origem):
    global estado, nao_visitados, entrada

    print(f"[{idx}] <-- {tipo} <-- {origem}  |  estado atual: {estado.name}")

    if estado == Estado.OCIOSO and tipo == 'T':
        entrada       = origem
        nao_visitados = [v for v in Nx if v != origem]  # remove quem enviou
        estado        = Estado.VISITADO
        visita()

    elif estado == Estado.VISITADO and tipo == 'T':
        # Remove origem dos não_visitados, pois esta aresta é back-edge
        if origem in nao_visitados:
            nao_visitados.remove(origem)
        print(f"[{idx}] back-edge detectado com {origem}, enviando B")
        envia('B', origem)

    elif estado == Estado.VISITADO and tipo == 'R':
        visita()

    elif estado == Estado.VISITADO and tipo == 'B':
        visita()

    elif estado == Estado.OK:
        print(f"[{idx}] (mensagem {tipo} ignorada — já em estado OK)")

    else:
        print(f"[{idx}] AVISO: combinação não esperada estado={estado.name} tipo={tipo}")

# Callback do AMQP manda para os handlers acima
def callback(ch, method, properties, corpo):
    """
    Formato esperado das mensagens:
        "STARTER:<qualquer_coisa>"  → evento espontâneo
        "<ORIGEM>:<TIPO>"           → T, R ou B vindo de outro nó
    """
    texto  = corpo.decode()
    partes = texto.split(':', 1)   # separa só no primeiro ':'

    if len(partes) < 2:
        print(f"[{idx}] mensagem malformada: {texto}")
        return

    origem_ou_starter = partes[0]
    conteudo          = partes[1]

    if origem_ou_starter.upper() == 'STARTER':
        espontaneamente(conteudo)
    else:
        recebendo(conteudo, origem_ou_starter)

# Loop principal
canal.basic_consume(
    queue=idx,
    on_message_callback=callback,
    auto_ack=True
)

print(f"[{idx}] aguardando mensagens...")

try:
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()
print(f"[{idx}] encerrado.")
