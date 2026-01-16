import json
import time
import requests
import re

INPUT_FILE = "partita_raw.json"  
FLUENT_BIT_URL = "http://fluent-bit:9880/nba.log"
LOG_FILE_PATH = "/app/logs/nba_events.jsonl"
TIME_DELAY = 2 

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
    action_number = raw_event.get('actionNumber')
    
    action_type = raw_event.get('actionType', 'Unknown')
    description = raw_event.get('description', '')
    
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
        "team_code": raw_event.get('teamTricode'), 
        "event_type": action_type,
        "player_name": raw_event.get('playerNameI'),
        "shot_value": shot_value,
        "shot_result": shot_result,
        "assist_player": assist_player, 
        "x": x,
        "y": y,
    }
    return payload


def replay_from_file():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("Il file non contiene un JSON valido.")
            return

    game_meta = data.get('game', {})
    game_id = game_meta.get('gameId', 'UNKNOWN_GAME')
    eventi = game_meta.get('actions', [])
    
    for evento in eventi:
        evento_pulito = trasforma_evento(evento, game_id)
        
        if evento_pulito['player_name']:
            send_to_fluentbit(evento_pulito)
            time.sleep(TIME_DELAY)

if __name__ == "__main__":
    reset_log_file()
    replay_from_file()