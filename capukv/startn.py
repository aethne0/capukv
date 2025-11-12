#!/usr/bin/python

import subprocess
import sys
import uuid

def run(s): 
    subprocess.run(s, shell=True, check=True)

def haha(arr, exclude_index):
    return ",".join(s for i, s in enumerate(arr))

run("rm -rf /tmp/capukv-temp-x*")

if sys.argv[3] == "rel":
    rel = "--release"
else:
    rel = ""


starting_port = int(sys.argv[1])
count = int(sys.argv[2])
peer_uris = [f'http://0.0.0.0:{port}' for port in range(starting_port, starting_port+count)]
peer_ids = [str(uuid.uuid4()) for _ in range(starting_port, starting_port+count)]
peer_ports = [port for port in range(starting_port, starting_port+count)]
http_ports = [port for port in range(starting_port+count, starting_port+count+count)]
window_name = "capukv"
print("->", window_name)

#sys.exit()

# new window
run(f'tmux new-window -n "{window_name}" -d')
# start first pane in the new window
path = f'/tmp/capukv-temp-x{0}'
run(f'tmux send-keys -t "{window_name}.0" "cargo run --bin capukv {rel} -- --dir {path} --id {peer_ids[0]} --port {starting_port} --api-port {starting_port+count} --peers {haha(peer_uris, 0)} --peer-ids {haha(peer_ids, 0)}" C-m')

for i, port in enumerate(peer_ports[1:], start=1):
    target_pane = i - 1
    our_id = peer_ids[i]
    current_peers =  haha(peer_uris, i)
    current_ids =  haha(peer_ids, i)
    http_port = http_ports[i]

    # odd index -> horizontal split, even -> vertical split
    name = f'{window_name}.{str(target_pane)}'
    path = f'/tmp/capukv-temp-x{i}'
    if i % 2 == 1:
        run(f'tmux split-window -h -t "{name}" "cargo run --bin capukv {rel} -- --dir {path} --id {our_id} --api-port {http_port} --port {port} --peers {current_peers} --peer-ids {current_ids}"')
    else:
        run(f'tmux split-window -v -t "{name}" "cargo run --bin capukv {rel} -- --dir {path} --id {our_id} --api-port {http_port} --port {port} --peers {current_peers} --peer-ids {current_ids}"')

run(f'tmux select-layout -t "{window_name}" tiled')
run(f'tmux select-window -t "{window_name}"')
