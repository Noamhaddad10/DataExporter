"""
send_fire_pdu.py
================
Simulateur FirePdu DIS 7 (PDU type 2) — construction manuelle via struct.pack.
Envoie des paquets UDP vers le serveur logger.py sur localhost:3000.

NOTE : opendis est utilisé côté serveur pour le PARSING uniquement.
       Ce script construit les bytes manuellement (sérialisation opendis buggée).

Structure binaire complète — 98 bytes, big-endian (!)
  PduSuperclass    12 bytes  !BBBBIHH
  Pdu extension     2 bytes  !BB
  WarfareFamilyPdu 12 bytes  !HHHHHH
  munitionExpendibleID  6 bytes  !HHH
  eventID           6 bytes  !HHH
  fireMissionIndex  4 bytes  !I
  location         24 bytes  !ddd
  descriptor       16 bytes  !BBHBBBBHHHH
  velocity         12 bytes  !fff
  range             4 bytes  !f
                  ─────────
                   98 bytes
"""

import struct
import socket
import time

# ============================================================
# CONFIGURATION — modifier ici sans toucher au reste du script
# ============================================================
HOST           = 'localhost'
PORT           = 3000
EXERCISE_ID    = 9       # Doit correspondre à DataExporterConfig.json → exercise_id
NUM_MESSAGES   = 10      # Nombre de FirePdu à envoyer
DELAY_SECONDS  = 0.5     # Délai entre chaque envoi (secondes)
PDU_LENGTH     = 98      # Taille totale du paquet FirePdu en bytes

# --- Entity IDs ---
FIRING_SITE,   FIRING_APP,   FIRING_ENTITY   = 1, 3101, 1    # Tireur
TARGET_SITE,   TARGET_APP,   TARGET_ENTITY   = 1, 3101, 2    # Cible
MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY = 1, 3101, 100  # Munition
EVENT_SITE,    EVENT_APP                     = 1, 3101        # Base eventID

# --- Coordonnées geocentriques ECEF (Israel ~32°N 34°E) ---
LOCATION_X = 4_429_530.0   # mètres
LOCATION_Y = 3_094_568.0
LOCATION_Z = 3_320_580.0

# --- MunitionDescriptor / EntityType ---
ENTITY_KIND = 2     # 2 = Munition
DOMAIN      = 2     # 2 = Air
COUNTRY     = 105   # 105 = Israël
CATEGORY    = 1
SUBCATEGORY = 1
SPECIFIC    = 1
EXTRA       = 0
WARHEAD     = 1000  # Warhead type
FUSE        = 100   # Fuse type
QUANTITY    = 1
RATE        = 1

# --- Vecteur vitesse (m/s) ---
VEL_X, VEL_Y, VEL_Z = 100.0, 0.0, -50.0

# --- Portée (mètres) ---
RANGE = 5000.0


# ============================================================
# CONSTRUCTION DU PAQUET
# ============================================================

def build_fire_pdu(event_number: int, timestamp: int) -> bytes:
    """
    Construit un FirePdu DIS 7 complet en bytes bruts.

    Args:
        event_number : numéro d'événement (incrémenté à chaque envoi)
        timestamp    : timestamp DIS 32-bit unsigned

    Returns:
        bytes de 98 octets, big-endian
    """

    # ----------------------------------------------------------
    # 1. PduSuperclass — 12 bytes
    #    Format : !BBBBIHH
    #      protocolVersion (B)  exerciseID (B)  pduType (B)  protocolFamily (B)
    #      timestamp (I)  length (H)  padding (H)
    # ----------------------------------------------------------
    pdu_superclass = struct.pack('!BBBBIHH',
        7,            # protocolVersion  — DIS 7 (IEEE 1278.1-2012)
        EXERCISE_ID,  # exerciseID       — filtre du serveur (doit == config)
        2,            # pduType          — FirePdu = 2  ← filtre serveur packet[2]
        2,            # protocolFamily   — Warfare Family = 2
        timestamp,    # timestamp        — 32-bit unsigned big-endian
        PDU_LENGTH,   # length           — taille totale du PDU en bytes
        0,            # padding          — réservé, = 0
    )  # → 12 bytes

    # ----------------------------------------------------------
    # 2. Pdu extension — 2 bytes
    #    pduStatus (B)  padding (B)
    # ----------------------------------------------------------
    pdu_ext = struct.pack('!BB',
        0,  # pduStatus — pas de flag particulier
        0,  # padding
    )  # → 2 bytes

    # ----------------------------------------------------------
    # 3. WarfareFamilyPdu — 12 bytes
    #    firingEntityID (HHH) + targetEntityID (HHH)
    # ----------------------------------------------------------
    warfare = struct.pack('!HHHHHH',
        FIRING_SITE, FIRING_APP, FIRING_ENTITY,  # firingEntityID  : site / app / entity
        TARGET_SITE, TARGET_APP, TARGET_ENTITY,  # targetEntityID  : site / app / entity
    )  # → 12 bytes

    # ----------------------------------------------------------
    # 4. munitionExpendibleID (EntityID) — 6 bytes
    #    siteID (H)  applicationID (H)  entityID (H)
    # ----------------------------------------------------------
    munition_expendable_id = struct.pack('!HHH',
        MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY,
    )  # → 6 bytes

    # ----------------------------------------------------------
    # 5. eventID (EventIdentifier) — 6 bytes
    #    simulationAddress.siteID (H)  simulationAddress.applicationID (H)
    #    eventNumber (H)
    # ----------------------------------------------------------
    event_id = struct.pack('!HHH',
        EVENT_SITE, EVENT_APP, event_number,
    )  # → 6 bytes

    # ----------------------------------------------------------
    # 6. fireMissionIndex — 4 bytes
    #    Unsigned int — on utilise l'event_number comme index
    # ----------------------------------------------------------
    fire_mission_index = struct.pack('!I', event_number)  # → 4 bytes

    # ----------------------------------------------------------
    # 7. locationInWorldCoordinates (Vector3Double) — 24 bytes
    #    x (d)  y (d)  z (d)  — coordonnées ECEF en mètres
    # ----------------------------------------------------------
    location = struct.pack('!ddd',
        LOCATION_X,
        LOCATION_Y,
        LOCATION_Z,
    )  # → 24 bytes

    # ----------------------------------------------------------
    # 8. MunitionDescriptor — 16 bytes
    #    munitionType / EntityType (8 bytes) :
    #      entityKind (B)  domain (B)  country (H)
    #      category (B)  subcategory (B)  specific (B)  extra (B)
    #    warhead (H)  fuse (H)  quantity (H)  rate (H)
    # ----------------------------------------------------------
    descriptor = struct.pack('!BBHBBBBHHHH',
        ENTITY_KIND, DOMAIN, COUNTRY,               # EntityType — identité de la munition
        CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA,     # EntityType — détails
        WARHEAD, FUSE, QUANTITY, RATE,              # paramètres de tir
    )  # → 16 bytes

    # ----------------------------------------------------------
    # 9. velocity (Vector3Float) — 12 bytes
    #    x (f)  y (f)  z (f)  — vecteur vitesse en m/s
    # ----------------------------------------------------------
    velocity = struct.pack('!fff',
        VEL_X, VEL_Y, VEL_Z,
    )  # → 12 bytes

    # ----------------------------------------------------------
    # 10. range (float) — 4 bytes
    #     Distance de tir en mètres
    # ----------------------------------------------------------
    range_field = struct.pack('!f', RANGE)  # → 4 bytes

    # --- Assemblage du paquet complet ---
    packet = (
          pdu_superclass        # 12
        + pdu_ext               #  2
        + warfare               # 12
        + munition_expendable_id #  6
        + event_id              #  6
        + fire_mission_index    #  4
        + location              # 24
        + descriptor            # 16
        + velocity              # 12
        + range_field           #  4
    )                           # = 98 bytes

    # --- Vérifications d'intégrité ---
    assert len(packet) == PDU_LENGTH, (
        f"Taille PDU incorrecte : {len(packet)} bytes (attendu {PDU_LENGTH})"
    )
    assert packet[2] == 2, (
        f"pduType incorrect : {packet[2]} (attendu 2 = FirePdu)"
    )

    return packet


# ============================================================
# BOUCLE D'ENVOI
# ============================================================

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Timestamp de base : secondes Unix tronquées à 32 bits (DIS convention)
    base_timestamp = int(time.time()) & 0xFFFFFFFF

    print("=" * 60)
    print(f"  Simulateur FirePdu DIS 7")
    print(f"  Destination  : {HOST}:{PORT}")
    print(f"  exerciseID   : {EXERCISE_ID}")
    print(f"  PDU size     : {PDU_LENGTH} bytes")
    print(f"  Messages     : {NUM_MESSAGES}  --  delai : {DELAY_SECONDS}s")
    print("=" * 60)

    for i in range(1, NUM_MESSAGES + 1):
        event_number = i
        timestamp = (base_timestamp + i) & 0xFFFFFFFF

        packet = build_fire_pdu(event_number, timestamp)
        sock.sendto(packet, (HOST, PORT))

        print(
            f"  FirePdu #{i:3d} envoyé — "
            f"attacker {FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY} -> "
            f"target {TARGET_SITE}:{TARGET_APP}:{TARGET_ENTITY}, "
            f"event {EVENT_SITE}:{EVENT_APP}:{event_number}"
        )

        if i < NUM_MESSAGES:
            time.sleep(DELAY_SECONDS)

    sock.close()
    print("=" * 60)
    print(f"  Terminé — {NUM_MESSAGES} FirePdu envoyés vers {HOST}:{PORT}")
    print("=" * 60)


if __name__ == '__main__':
    main()
