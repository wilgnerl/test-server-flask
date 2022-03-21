from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
import cmd_v2
import base64
import requests
import json
import os

token = os.environ.get("FLASK_TOKEN")

app = Flask(__name__)


def schedule_dowlink(payload, port):
  
    body = {}
    body["devEUI"] = "0100000000000001"
    body["payload"] = payload
    body["port"] = port
    print("Body dowlink: ", body)

    response = requests.post(
            url="https://connector.koretmdata.com.br/api/v2/downlinks",
            headers={"Authorization": f"{token}"},
            data=body,
        )
    return [response.text, response.status_code]


@app.route("/")
def index():
    return "server running"

@app.route("/test")
def test_redis():
    data = cmd_v2.list_all_keys()
    return f"Data: {data}"

@app.route("/test/remove")
def test_redis():
    cmd_v2.delete_all()
    return "Deletado"



@app.route("/server", methods=["POST", "GET"])
def server():
    
    # port = 42
    
    try:
        if request.method == "POST":
            # VERIFICA SE TEM DADOS NO BODY
            if request.data:
                # VERIFICA SE VEIO PAYLOAD NO BODY
                print(json.loads(request.data))
                if "payload" in json.loads(request.data)["params"]:
                    identificador = json.loads(request.data)["meta"]["device"]
                    print(f"identificador: {identificador}")
                    # PEGA PAYLOAD
                    payload = json.loads(request.data)["params"]["payload"]
                    print(f"Payload: {payload}")
                    
                    # DECODIFICA PAYLOAD
                    payloadDecoded = base64.b64decode(payload)
                    print(f"Payload Decodificado: {payloadDecoded}")

                    # MANDA PRO TESTE
                    responseDecode = cmd_v2.decode(payloadDecoded)
                    print(f"Response decode: {responseDecode}")

                    # MANDA PRO PROCESSO
                    responseProcess = cmd_v2.process(responseDecode, identificador=identificador)
                    print(f"Response process: {responseProcess}")

                    # PEGA RESPOSTA DO TESTE E CODIFICA PARA BASE64
                    payloadDownlink = base64.b64encode(responseProcess)
                    print(f"Payload base64 to downlink: {payloadDownlink}")
                    
                    response = schedule_dowlink(
                        payload=payloadDownlink, port=10
                    )
                    
                    print("Downlink: ", response)
                    
                else:
                    return Response("", status=201, mimetype="application/json")
        return Response("", status=201, mimetype="application/json")
    
    except OSError as err:
        print(err)
        return Response(f"{err}", status=404, mimetype="application/json")

if __name__ == "__main__":
    app.debug = True
    app.run()
