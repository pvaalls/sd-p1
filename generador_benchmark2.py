import random
import uuid

# Configuració del benchmark
TOTAL_PETICIONS = 60000
TOTAL_SEIENTS = 20000
FITXER_SORTIDA = "2benchmark_hotspot_60000.txt"

# Càlcul de la distribució (80% peticions al 5% dels seients)
SEIENTS_HOTSPOT = int(TOTAL_SEIENTS * 0.05)      # 1.000 seients molt demandats
PETICIONS_HOTSPOT = int(TOTAL_PETICIONS * 0.80)  # 48.000 peticions cap a aquests seients
PETICIONS_NORMALS = TOTAL_PETICIONS - PETICIONS_HOTSPOT # 12.000 peticions

def generar_benchmark():
    # Llistes de seients (prefix 'S_')
    seients_calents = [f"S_{i}" for i in range(SEIENTS_HOTSPOT)]
    seients_normals = [f"S_{i}" for i in range(SEIENTS_HOTSPOT, TOTAL_SEIENTS)]

    peticions = []

    # Generar el 80% de les peticions cap al 5% dels seients (Hotspots)
    for _ in range(PETICIONS_HOTSPOT):
        client_id = f"Client_{uuid.uuid4().hex[:6]}"
        seat_id = random.choice(seients_calents)
        request_id = f"Req_{uuid.uuid4().hex[:8]}"
        peticions.append(f"BUY {client_id} {seat_id} {request_id}\n")

    # Generar el 20% de les peticions cap al 95% dels seients restants
    for _ in range(PETICIONS_NORMALS):
        client_id = f"Client_{uuid.uuid4().hex[:6]}"
        seat_id = random.choice(seients_normals)
        request_id = f"Req_{uuid.uuid4().hex[:8]}"
        peticions.append(f"BUY {client_id} {seat_id} {request_id}\n")

    # Barrejar les peticions aleatòriament perquè no estiguin totes juntes al principi
    random.shuffle(peticions)

    # Escriure al fitxer
    with open(FITXER_SORTIDA, 'w') as f:
        f.writelines(peticions)

    print(f"Fitxer '{FITXER_SORTIDA}' generat amb èxit.")
    print(f"- Peticions totals: {TOTAL_PETICIONS}")
    print(f"- Seients 'Hotspot' (alta contenció): {SEIENTS_HOTSPOT} seients rebran {PETICIONS_HOTSPOT} peticions.")

if __name__ == "__main__":
    generar_benchmark()