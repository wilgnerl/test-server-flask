import struct
import pprint 
import time
import random
import math 
import base64
import redis
import os

r = redis.Redis.from_url(os.environ.get("REDIS_URL"))

WF_UPLOAD_PORT = 10

SVR_ANSWER_PROB     = math.sqrt(0.7) # up*down prob.
SVR_REQUEST_WF_PROB = 0.20

CMD_STATUS        = 0
CMD_STATUS_ACK    = 1
CMD_UPL_BEGIN     = 2
CMD_UPL_BEGIN_ACK = 3
CMD_UPL_SEGM      = 4
CMD_UPL_SEGM_ACK  = 5
CMD_UPL_END       = 6
CMD_UPL_END_ACK   = 7
CMD_UPL_SEGM_REQ  = 8

UPLOADED_DATA = []
SEGM_IDXS_BLOCK_SIZE = 10

### CREATE BY Wilgner - Redis Functions
def check_if_key_exists(key):
    key_existis = r.exists(key)
    r.expire(key, 21600)
    if key_existis == 1:
        print("CHAVE CONSULTADA COM SUCESSO")
        return True
    else:
        return False
    
def push_itens_in_key(key, list_index):
    for item in list_index:
        r.lpush(key, item, )
        r.expire(key, 21600)
    
    print("LISTA RETORNADA COM SUCESSO")
    return True

def get_list_of_index_by_key(key):
    lista = [int(i) for i in r.lrange(key, 0, -1)]
    print(f"lista {lista}")
    return lista

def remove_index_of_list(key, index):
    try:
        print(index)
        r.lrem(key, -1, index)
        return True
    except:
        return False

def delete_key(key):
    r.delete(key)
    return True

def list_all_keys():
    return r.keys()

def delete_all():
    return r.flushdb()
### CREATE BY Wilgner -  Redis Functions


def svr_will_answer():
    return (random.random() < SVR_ANSWER_PROB)

def svr_will_req_wf():
    return (random.random() < SVR_REQUEST_WF_PROB)

def create_byte_array(dstr):
    data = [ int(dstr[p:p+2],16) for p in range(0,len(dstr),2) ]
    data = bytes(data)

    return data

def dump(data,label=None):
    if label:
        print('\r\n' + label + ':')
    a = [ '%02X' % v for v in data ]
    s = ''.join(a)
    print(s)
    a = [ '0x%02X' % v for v in data ]
    s = '{ ' + ', '.join(a) + ' }'
    print(s)

def dump_dict(d,label=None):
    pp = pprint.PrettyPrinter(indent=4,width=80,compact=True,sort_dicts=True)
    if label:
        print('\r\n' + label + ':')
    pp.pprint(d)

def decode_status(data):
    if len(data) == 0:
        return None
        
    indications = data[0]
    alarm_a = (indications & 0x01) > 0
    alarm_b = (indications & 0x02) > 0
    payload = { 'alarm_a': alarm_a, 'alarm_b': alarm_b }

    keys = ['accel_x_rms','accel_y_rms','accel_z_rms','giros_x_rms','giros_y_rms','giros_z_rms']

    size = 1
    for bit_pos in range(2,8):
        if (indications & (1 << bit_pos)) > 0:
            size = size + 2
    
    if size != len(data):
        return None

    sensor = 0
    for bit_pos in range(2,8):      
        if (indications & (1 << bit_pos)) > 0:
            payload[keys[bit_pos - 2]] = struct.unpack('>H',data[sensor*2+1:sensor*2+3])[0]
            sensor = sensor + 1

    return payload

def decode_upload_begin(data):
    if len(data) < 9:
        return None

    (full_size,num_segms,segm_size,indications) = struct.unpack('>LHHB',data[:9])

    payload= {'full_size':full_size, 'num_segms':num_segms, 'segm_size':segm_size}

    keys = ['accel_x_size','accel_y_size','accel_z_size','giros_x_size','giros_y_size','giros_z_size']

    size = 9
    for bit_pos in range(2,8):
        if (indications & (1 << bit_pos)) > 0:
            size = size + 2    

    if size != len(data):
        return None

    sensor = 0
    for bit_pos in range(2,8):      
        if (indications & (1 << bit_pos)) > 0:
            payload[keys[bit_pos - 2]] = struct.unpack('>H',data[sensor*2+9:sensor*2+11])[0]
            sensor = sensor + 1     

    return payload   

def decode_upload_segm(data):
    if len(data) < 4:
        return None

    (segm_idx,segm_size) = struct.unpack('>HH',data[:4])

    payload= {'segm_idx':segm_idx, 'segm_size':segm_size}

    size = 4 + segm_size
    if size != len(data):
        return None

    segm_data = [ d for d in data[4:]]
    payload['data'] = segm_data

    return payload 

def decode_upload_end(data):
    if len(data) != 0:
        return None
    
    return {}

def encode_status_ack(cmd):
    data = []
    data.append(cmd['id'])

    indication = 0

    if cmd['alarm_a']: indication = indication | (1 << 0);
    if cmd['alarm_b']: indication = indication | (1 << 1);
    if cmd['accel_x']: indication = indication | (1 << 2);
    if cmd['accel_y']: indication = indication | (1 << 3);
    if cmd['accel_z']: indication = indication | (1 << 4);
    if cmd['giros_x']: indication = indication | (1 << 5);
    if cmd['giros_y']: indication = indication | (1 << 6);
    if cmd['giros_z']: indication = indication | (1 << 7);
    
    data.append(indication)
    data = bytes(data)

    return data

def encode_upl_begin_ack(cmd):
    data = []
    data.append(cmd['id'])
    data = bytes(data)

    return data

def encode_upl_segm_ack(cmd):
    data = []
    data.append(cmd['id'])
    for b in struct.pack('>B',len(cmd['segm_idx'])):
        data.append(b)
    for idx in cmd['segm_idx']:
        for b in struct.pack('>H',idx):
            data.append(b)
    data = bytes(data)

    return data

def encode_upl_segm_req(cmd):
    data = []
    data.append(cmd['id'])
    for b in struct.pack('>B',len(cmd['segm_idx'])):
        data.append(b)
    for idx in cmd['segm_idx']:
        for b in struct.pack('>H',idx):
            data.append(b)
    data = bytes(data)

    return data
    
def encode_upl_end_ack(cmd):
    data = []
    data.append(cmd['id'])
    data = bytes(data)

    return data

def decode(data):
    if len(data) == 0:
        return {}

    cmd = { 'id': data[0] } 
    data = data[1:]
    payload = {}

    if cmd['id'] == CMD_STATUS:
        payload = decode_status(data)
    elif cmd['id'] == CMD_UPL_BEGIN:
        payload = decode_upload_begin(data)
    elif cmd['id'] == CMD_UPL_SEGM:
        payload = decode_upload_segm(data)
    elif cmd['id'] == CMD_UPL_END:
        payload = decode_upload_end(data)

    if payload is not None:
        cmd.update(payload)
    else:
        payload = None
    
    return cmd

def auto_test():

    print('--== Testing decoders ==--')

    data = create_byte_array(r'001D000A000B000C')
    d = decode(data)
    dump_dict(d,'CMD_STATUS')

    data = create_byte_array(r'0200001800003000809C0800040008000400')
    d = decode(data)
    dump_dict(d,'CMD_UPL_BEGIN')

    data = create_byte_array(r'04004000140000000000000000000000000000000000000000')
    d = decode(data)
    dump_dict(d,'CMD_UPL_SEGM')

    data = create_byte_array(r'06')
    d = decode(data)
    dump_dict(d,'CMD_UPL_END')

    print('--== Testing encoders ==--')

    cmd = { 'id': CMD_STATUS_ACK,
            'alarm_a':True, 'alarm_b': False,
            'accel_x': True, 'accel_y': True, 'accel_z': True,
            'giros_x': False, 'giros_y':False, 'giros_z': True }
    data = encode_status_ack(cmd)
    dump(data,'CMD_STATUS_ACK')

    cmd = { 'id': CMD_UPL_BEGIN_ACK }
    data = encode_upl_begin_ack(cmd)
    dump(data,'CMD_UPL_BEGIN_ACK')

    segm_idx = [ 16, 32, 64 ]
    cmd = { 'id': CMD_UPL_SEGM_ACK, 'segm_idx': segm_idx }
    data = encode_upl_segm_ack(cmd)
    dump(data,'CMD_UPL_SEGM_ACK')

    cmd = { 'id': CMD_UPL_END_ACK }
    data = encode_upl_end_ack(cmd)
    dump(data,'CMD_UPL_END_ACK')

    segm_idx = [ 16, 32, 64 ]
    cmd = { 'id': CMD_UPL_SEGM_REQ, 'segm_idx': segm_idx }
    data = encode_upl_segm_req(cmd)
    dump(data,'CMD_UPL_SEGM_REQ')

    cmd = { 'id': CMD_UPL_END_ACK }
    data = encode_upl_end_ack(cmd)
    dump(data,'CMD_UPL_END_ACK')

def process(cmd, identificador):
    global UPLOADED_DATA
    data = bytes([])
    if cmd['id'] == CMD_STATUS:
        ans = { 'id': CMD_STATUS_ACK, 'alarm_a':cmd['alarm_a'], 'alarm_b':cmd['alarm_b'] }
        if ans['alarm_a'] or ans['alarm_b']:
            ans['accel_x'] = 'accel_x_rms' in cmd
            ans['accel_y'] = 'accel_y_rms' in cmd
            ans['accel_z'] = 'accel_z_rms' in cmd
            ans['giros_x'] = 'giros_x_rms' in cmd
            ans['giros_y'] = 'giros_y_rms' in cmd
            ans['giros_z'] = 'giros_z_rms' in cmd
        else:
            ans['accel_x'] = False
            ans['accel_y'] = False
            ans['accel_z'] = False
            ans['giros_x'] = False
            ans['giros_y'] = False
            ans['giros_z'] = False

        # force wave form request even when it is not indicated (sometimes)
        if not(ans['alarm_a'] or ans['alarm_b']) and svr_will_req_wf():
            print("==> REQUESTING WAVE FORM (SIMUL)")
            ans['alarm_a'] = True
            ans['accel_x'] = svr_will_req_wf()
            ans['accel_y'] = svr_will_req_wf()
            ans['accel_z'] = svr_will_req_wf()
            ans['giros_x'] = svr_will_req_wf()
            ans['giros_y'] = svr_will_req_wf()
            ans['giros_z'] = svr_will_req_wf()

        dump_dict(cmd,'CMD_STATUS')
        dump_dict(ans,'CMD_STATUS_ACK')

        data = encode_status_ack(ans)

    elif cmd['id'] == CMD_UPL_BEGIN:
        # Create a full list with all indexes. Each index will be removed when a proper
        # segment upload message is received. Remaining indexes are missing and need to 
        # be requested.
        # UPLOADED_DATA = list(range(0,cmd['num_segms']))
        
        ####### ADAPTADO POR WILGNER
        
        sensor_exists = check_if_key_exists(identificador)
        
        ####### ADAPTADO POR WILGNER
        if sensor_exists:
            push_itens_in_key(identificador, list(range(0,cmd['num_segms'])))
        
        else:
            push_itens_in_key(identificador, list(range(0,cmd['num_segms'])))
            

        ans = { 'id': CMD_UPL_BEGIN_ACK }

        dump_dict(cmd,'CMD_STATUS')
        dump_dict(ans,'CMD_STATUS_ACK')

        data = encode_upl_begin_ack(ans)
    
    elif cmd['id'] == CMD_UPL_SEGM:
        try:
            # ADAPTADO POR WILGNER
            
            UPLOADED_DATA = get_list_of_index_by_key(identificador)
            # UPLOADED_DATA.remove(cmd['segm_idx'])
            remove_index_of_list(identificador, cmd['segm_idx'])
        except ValueError:
            pass

        #ans = { 'id': CMD_UPL_SEGM_ACK, 'segm_idx':[ cmd['segm_idx'] ] }

        dump_dict(cmd,'CMD_UPL_SEGM')
        #dump_dict(ans,'CMD_UPL_SEGM_ACK')
        
        #data = encode_upl_segm_ack(ans)

    elif cmd['id'] == CMD_UPL_END:
        dump_dict(cmd,'CMD_UPL_END')
        # ADAPTADO POR WILGNER
        UPLOADED_DATA = get_list_of_index_by_key(identificador)
        
        if UPLOADED_DATA:
            #print("Missing segments {}".format(str(UPLOADED_DATA)))
            # if there are remaining indexes, request them in blocks of SEGM_IDXS_BLOCK_SIZE
            segm_idxs = UPLOADED_DATA[:SEGM_IDXS_BLOCK_SIZE]
            ans = { 'id': CMD_UPL_SEGM_REQ, 'segm_idx':segm_idxs }
            dump_dict(ans,'CMD_UPL_SEGM_REQ')
            data = encode_upl_segm_req(ans)
        else:
            #print("No missing segments!")
            ans = { 'id': CMD_UPL_END_ACK }
            dump_dict(ans,'CMD_UPL_END_ACK')
            data = encode_upl_end_ack(ans)
            
            # ADAPTADO POR WILGNER
            delete_key(identificador)

    else:
        print("Invalid command {}".format(cmd['id']))

    return data

def server():
    context = zmq.Context()

    data_sub = context.socket(zmq.SUB)
    data_sub.connect("tcp://172.19.84.39:60000")
    data_sub.setsockopt_string(zmq.SUBSCRIBE,"")

    data_pub = context.socket(zmq.PUB)
    data_pub.bind("tcp://172.19.84.39:60001")

    while True:
        try:
            data = data_sub.recv(zmq.DONTWAIT)
        except zmq.error.Again:
            data = None
        
        if data:
            if svr_will_answer(): # simulation only
                cmd = decode(data)
                if cmd:
                    data = process(cmd)
                    if data:
                        if svr_will_answer(): # simulation only
                            data_pub.send(data)                    
                        else:
                            print("==> MISSING DOWNLOAD PACKET (SIMUL)")
            else:
                print("==> MISSING UPLOAD PACKET (SIMUL)")
        else:
            time.sleep(0.5)

# Possivel implementation para uma funcao lambda.
# Provavelmente o dado deve ser json e vai precisar ser
# tratado de outra forma, estou resumindo a payload em 
# base 64 e porta
def server_lambda(port,data):
    # check application port before decoding
    if port != WF_UPLOAD_PORT:
        return

    # decode payload
    data = base64.b64decode(data)
    cmd = decode(data)
    if cmd:
        data = process(cmd)
        if data:
            data = base64.b64encode(data)
            # send_lora(data) #<<== colocar sua funcao de envio

# if __name__ == "__main__":
#     server()
