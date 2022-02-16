from textwrap import indent
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
import cmd
import base64
import requests
import json
import os


token = os.environ.get("FLASK_TOKEN")

app = Flask(__name__)


@app.route("/")
def index():
    return "server running"


def schedule_dowlink(devEUI, payload, port):

    body = {}
    body["devEUI"] = devEUI
    body["payload"] = payload
    body["port"] = port

    res = requests.post(
        url="https://connector.koretmdata.com.br/api/v2/downlinks",
        headers={"Authorization": f"{token}"},
        data=body,
    )
    return json.dumps(res.json(), indent=3)


@app.route("/server", methods=["POST", "GET"])
def server():
    
    # port = 42
    
    try:
        if request.method == "POST":
            # VERIFICA SE TEM DADOS NO BODY
            if request.data:
                # VERIFICA SE VEIO PAYLOAD NO BODY
                if json.loads(request.data)["params"]["payload"]:
                    print(json.loads(request.data)["params"])
                    # PEGA PAYLOAD
                    payload = json.loads(request.data)["params"]["payload"]
                    print(f"Payload: {payload}")
                    # DECODIFICA PAYLOAD
                    payloadDecoded = base64.b64decode(payload)
                    print(f"Payload Decodificado: {payloadDecoded}")

                    # MANDA PRO TESTE
                    responseDecode = cmd.decode(payloadDecoded)
                    print(f"Response decode: {responseDecode}")

                    # MANDA PRO PROCESSO
                    responseProcess = cmd.process(responseDecode)
                    print(f"Response process: {responseProcess}")

                    # PEGA RESPOSTA DO TESTE E CODIFICA PARA BASE64
                    payloadDownlink = base64.b64encode(responseProcess)
                    print(f"Payload base64 to downlink: {payloadDownlink}")

                    # response = schedule_dowlink(
                    #     devEUI=devEUI, payload=payloadDownlink, port=port
                    # )
                    # print(response)

            return Response("", status=201, mimetype="application/json")
    except:
        return Response("Error", status=404, mimetype="application/json")

if __name__ == "__main__":
    app.debug = True
    app.run()
