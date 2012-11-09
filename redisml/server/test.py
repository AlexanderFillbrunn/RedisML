from server import *
import client_proxy

SERVER.clients.append(client_proxy.Client_proxy("local", "127.0.0.1", 2))
SERVER.send("Test", 12)
SERVER.send("Test2", 12)