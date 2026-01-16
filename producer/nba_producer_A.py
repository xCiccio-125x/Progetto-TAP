import requests
import json
import re
import time
import os
import sys

# --- CONFIGURAZIONE ---

GAME_ID =  os.getenv("GAME_ID", "0")
NBA_URL = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{GAME_ID}.json"
FLUENT_BIT_URL = 'http://fluent-bit:9880/nba.log'
LOG_FILE_PATH = "/app/logs/nba_events.jsonl"
TIME_INTERVAL = 10 

def send_to_fluentbit(payload):
    try:
        with open(LOG_FILE_PATH, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as e:
        print(f"[!] Errore scrittura file: {e}")
        
def reset_log_file():
    try:
        with open(LOG_FILE_PATH, "w") as f:
            pass
    except Exception as e:
        print(f"Errore durante il reset del log: {e}")

def clean_clock(raw_clock):
    if not raw_clock: return "00:00"
    c = raw_clock.replace('PT', '').replace('M', ':').replace('S', '')
    if '.' in c: c = c.split('.')[0]
    return c

def extract_assist(description):
    if not description: return None
    match = re.search(r'\(([^)]+)\s+\d+\s+AST\)', description)
    if match: return match.group(1).strip()
    return None

def trasforma_evento(raw_event, game_id):
    
    action_type = raw_event.get('actionType', 'Unknown')
    description = raw_event.get('description', '')
    tricode = raw_event.get('teamTricode')
    action_number = raw_event.get('actionNumber')

    shot_value = 0
    lower_type = action_type.lower()
    if "3pt" in lower_type: shot_value = 3
    elif "2pt" in lower_type: shot_value = 2
    elif "freethrow" in lower_type: shot_value = 1
        
    shot_result = raw_event.get('shotResult')
    
    assist_player = None
    if shot_result == "Made":
        assist_player = extract_assist(description)

    x = raw_event.get('x')
    y = raw_event.get('y')
    if x is not None: x = round(x, 1)
    if y is not None: y = round(y, 1)

    payload = {
        "game_id": game_id,
        "event_id": action_number,
        "quarter": raw_event.get('period'),
        "clock": clean_clock(raw_event.get('clock')),
        "team_code": tricode, 
        "event_type": action_type,
        "player_name": raw_event.get('playerNameI'),
        "shot_value": shot_value,
        "shot_result": shot_result,
        "assist_player": assist_player, 
        "x": x,
        "y": y,
    }
    return payload

def live_loop():
    
    if(GAME_ID == "0"):
        print("NESSUNA PARTITA SPECIFICATA.")
        print("Usa: PARTITA=0000000 docker-compose up")
        sys.exit(0)
        
    print(f"AVVIO MONITORAGGIO LIVE: {GAME_ID}", flush=True)
    
    last_processed_id = 0 # Cursore per tenere traccia di dove siamo arrivati

    while True:
        try:
            response = requests.get(NBA_URL, timeout=10)
            
            if response.status_code == 403:
                print("Partita non ancora iniziata (File JSON non creato). Riprovo tra poco...", flush=True)
                time.sleep(TIME_INTERVAL)
                continue

            data = response.json()
            game_meta = data.get('game', {})
            
            # Filtro solo i nuovi eventi
            all_actions = game_meta.get('actions', [])
            new_eventi = [a for a in all_actions if a.get('actionNumber') > last_processed_id]

            if new_eventi:  
                for evento in new_eventi:
                    evento_pulito = trasforma_evento(evento, GAME_ID)

                    if evento_pulito['player_name']:
                        send_to_fluentbit(evento_pulito)
                    
                    last_processed_id = evento.get('actionNumber')
        except Exception as e:
            print(f"Errore: {e}", flush=True)

        time.sleep(TIME_INTERVAL)

if __name__ == "__main__":
    reset_log_file()
    live_loop()