import sys 
import time
import asyncio
import aiohttp
import json 

API_KEY = 'AIzaSyCCPooTtMiOpsRdVIPs9UWY7tnc9bkoXos'
REQUEST_URL = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={0}&radius={1}&key={2}'
LOCALHOST = '127.0.0.1'

server_neighbors = {
    'Hill': ['Jaquez', 'Smith'],
    'Singleton': ['Jaquez', 'Smith', 'Campbell'],
    'Smith': ['Hill', 'Singleton', 'Campbell'],
    'Campbell': ['Singleton', 'Smith'],
    'Jaquez': ['Hill', 'Singleton']
}


# server_ports = {
#     'Hill': 11850,
#     'Jaquez': 11851,
#     'Smith': 11852,
#     'Campbell': 11853,
#     'Singleton': 11854
# }

# use for local testing
server_ports = {
    'Hill': 8000,
    'Jaquez': 8001,
    'Smith': 8002,
    'Campbell': 8003,
    'Singleton': 8004
}

client_info = {}

def get_time_diff(t1, t2):
    time_diff = str(float(t1) - float(t2))
    
    if time_diff[0] != '-':
        time_diff = '+' + time_diff
        return time_diff
    else:
        return time_diff

# ISO_6709 formatting of lat & long can different lengths
# break up coords string into tuple by determining sign pos
def parse_pos(coords):
    signs = ['-','+']
    sign_pos = []
    for i in range(len(coords)):
        if coords[i] in signs:
            sign_pos.append(i)
    
    # wrong sign positions or num of signs
    if (0 not in sign_pos or (len(coords) - 1) in sign_pos) and len(sign_pos) != 2:
        return None

    latitude = coords[sign_pos[0]:sign_pos[1]]
    longitude = coords[sign_pos[1]:]
    return (latitude,longitude)


def get_location(coords):
    latitude, longitude = parse_pos(coords)
    location = latitude + "," + longitude
    location = location.replace("+","")
    return location


async def request_locations(url_request):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(url_request) as response:
            return await response.json()



def check_WHATSAT_args(radius, info_bounds):
    if radius <= 50 and radius > 0 and info_bounds <= 20 and info_bounds > 0:
        return True
    else:
         return False


def check_message(message):
    if len(message) == 0:
        log.write("RECEIVED MESSAGE INVALID; AT SERVER " + server_name + ": " + message )
        return 'INV_MES'
    
    command = message[0]

    if command == 'FLOOD':
        if len(message) == 6:
            return command

    elif command == 'IAMAT':
        if len(message) == 4 and parse_pos(message[2]) != None:
            return command

    elif command == 'WHATSAT':
        if len(message) == 4:
            if check_WHATSAT_args(int(message[2]), int(message[3])):
                return command

    else:
        return 'INV_MES'



async def flood(server, message):
    log.write("SERVER " + server_name + " INITIALIZING FLOODING..." + "\n")
    for curr_server in server_neighbors[server]:
        log.write("Attempting to connect server " + curr_server + " to port " + str(server_ports[curr_server]) + "\n")

        try:
            reader,writer = await asyncio.open_connection(host=LOCALHOST, port=server_ports[curr_server], loop=event_loop)

            log.write("Connection of server " + curr_server + " to port " + str(server_ports[curr_server]) + ": SUCCESS. WRITING DATA...\n")
            writer.write(message.encode())
            await writer.drain()
            writer.write_eof()
            writer.close()
        except:
            log.write("Connection of server " + curr_server + " to port " + str(server_ports[curr_server]) + ": FAILURE.\n")
            pass


async def handle_IAMAT(message, time_received):
    client_ID = message[1]
    coords = message[2]
    time_sent = message[3]

    client_info[client_ID] = [coords, time_sent, time_received, server_name]
    time_diff = get_time_diff(time_received, time_sent)
    server_response = ' '.join([ 'AT', server_name, time_diff, ' '.join(message[1:]) ])

    flood_message = message[1:]
    flood_message.append(time_received)
    flood_message.append(server_name)

    flood_message = "FLOOD " + ' '.join(flood_message)
    asyncio.create_task(flood(server_name, flood_message))  # log inside flood()

    # f = open("iamat_response.txt", "w")
    # f.write(server_response)
    return server_response



async def handle_WHATSAT(message):
    client_ID = message[1]

    if client_ID not in client_info:
        server_response = "? " + ' '.join(message)
        return server_response

    radius = int(message[2])
    upper_bound = int(message[3])

    coords, client_time, server_time, clients_server_name = client_info[client_ID]
    location = get_location(coords)
    radius = str(radius * 1000)
    request_url = REQUEST_URL.format(location,radius,API_KEY)
    time_diff = get_time_diff(server_time, client_time)

    server_response = ' '.join(['AT', clients_server_name, time_diff, client_ID, coords, client_time])
    json_locations = await request_locations(request_url)
    json_locations['results'] = json_locations['results'][:upper_bound]
    server_response += "\n" + json.dumps(json_locations, indent=3) + "\n"

    # f = open("whatsat_response.txt", "w")
    # f.write(server_response)
    return server_response 
  

async def handle_FLOOD(message):
    client_ID = message[1]
    time_sent = message[3]

    if client_ID in client_info:
        client_time = client_info[client_ID][1]
        if client_time < time_sent:
            client_info[client_ID] = message[2:]
            flood_message = ' '.join(message)
            asyncio.create_task(flood(server_name, flood_message))

    else: 
        client_info[client_ID] = message[2:]
        flood_message = ' '.join(message)
        asyncio.create_task(flood(server_name, flood_message))


async def handle_connection(reader, writer):
    
    data = await reader.readline()
    received_message = data.decode()
    og_message = received_message
    time_received = time.time()

    # record server input
    log.write("RECEIVED AT SERVER " + server_name + ": " + received_message + "\n")

    received_message = received_message.strip() 
    received_message = received_message.split() 

    message_type = check_message(received_message)
    

    if message_type == 'INV_MES':
        server_response = "? " + og_message

    elif message_type == 'IAMAT':
        server_response = await handle_IAMAT(received_message, str(time_received))

    elif message_type == 'WHATSAT':
        server_response = await handle_WHATSAT(received_message)

    elif message_type == 'FLOOD':
        await handle_FLOOD(received_message)


    # record server response
    if message_type != 'FLOOD':
        log.write("SERVER " + server_name + " RESPONSE: " + server_response + "\n")
        writer.write(server_response.encode())
        await writer.drain()
        writer.write_eof()
        writer.close()

def main():
   
    if len(sys.argv) != 2:
        print("Error: Incorrect number of arguments. USAGE: python3 server.py [Hill|Singleton|Smith|Jaquez|Campbell]")
        exit()
    elif sys.argv[1] not in server_neighbors:
        print("Error: Invalid server ID. USAGE: python3 server.py [Hill|Singleton|Smith|Jaquez|Campbell]")
        exit()

    global event_loop
    global server_name
    global log

    server_name = sys.argv[1]
    log_file = server_name + "_log.txt"
    log = open(log_file, 'w+')
    log.truncate(0)  # clear previous logs  
   
    event_loop = asyncio.get_event_loop()
    coroutine = asyncio.start_server(handle_connection, host=LOCALHOST, port=server_ports[server_name], loop=event_loop)
    server = event_loop.run_until_complete(coroutine)

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        pass

    
    server.close()
    event_loop.run_until_complete(server.wait_closed())
    event_loop.close()
    log.close()

if __name__=='__main__':
    main()
