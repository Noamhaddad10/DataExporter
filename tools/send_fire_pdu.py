"""
send_fire_pdu.py
================
DIS 7 FirePdu simulator (PDU type 2) -- manual construction via struct.pack.
Sends UDP packets to the logger.py server on localhost:3000.

NOTE: opendis is used server-side for PARSING only.
      This script builds bytes manually (opendis serialization is buggy).

Full binary structure -- 96 bytes, big-endian (!)
  PDU header           12 bytes  !BBBBIHH
  firingEntityID        6 bytes  !HHH   (WarfareFamilyPdu)
  targetEntityID        6 bytes  !HHH   (WarfareFamilyPdu)
  munitionExpendableID  6 bytes  !HHH
  eventID               6 bytes  !HHH
  fireMissionIndex      4 bytes  !I
  location             24 bytes  !ddd
  descriptor           16 bytes  !BBHBBBBHHHH
  velocity             12 bytes  !fff
  range                 4 bytes  !f
                      ---------
                       96 bytes

CORRECTION: Previous version included 2 "pdu_ext" bytes after the header,
which shifted all fields by 2 bytes and corrupted serialization.
"""

import struct
import socket
import time

# ============================================================
# CONFIGURATION -- edit here without touching the rest of the script
# ============================================================
HOST           = 'localhost'
PORT           = 3000
EXERCISE_ID    = 9       # Must match DataExporterConfig.json -> exercise_id
NUM_MESSAGES   = 10      # Number of FirePdu to send
DELAY_SECONDS  = 0.5     # Delay between each send (seconds)
PDU_LENGTH     = 96      # Total FirePdu packet size in bytes (no pdu_ext)

# --- Entity IDs ---
FIRING_SITE,   FIRING_APP,   FIRING_ENTITY   = 1, 3101, 1    # Shooter
TARGET_SITE,   TARGET_APP,   TARGET_ENTITY   = 1, 3101, 2    # Target
MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY = 1, 3101, 100  # Munition
EVENT_SITE,    EVENT_APP                     = 1, 3101        # eventID base

# --- Geocentric ECEF coordinates (Israel ~32N 34E) ---
LOCATION_X = 4_429_530.0   # meters
LOCATION_Y = 3_094_568.0
LOCATION_Z = 3_320_580.0

# --- MunitionDescriptor / EntityType ---
ENTITY_KIND = 2     # 2 = Munition
DOMAIN      = 2     # 2 = Air
COUNTRY     = 105   # 105 = Israel
CATEGORY    = 1
SUBCATEGORY = 1
SPECIFIC    = 1
EXTRA       = 0
WARHEAD     = 1000  # Warhead type
FUSE        = 100   # Fuse type
QUANTITY    = 1
RATE        = 1

# --- Velocity vector (m/s) ---
VEL_X, VEL_Y, VEL_Z = 100.0, 0.0, -50.0

# --- Range (meters) ---
RANGE = 5000.0


# ============================================================
# PACKET CONSTRUCTION
# ============================================================

def build_fire_pdu(event_number: int, timestamp: int) -> bytes:
    """
    Build a complete DIS 7 FirePdu as raw bytes.

    Args:
        event_number : event number (incremented with each send)
        timestamp    : 32-bit unsigned DIS timestamp

    Returns:
        96-byte big-endian bytes
    """

    # ----------------------------------------------------------
    # 1. PDU header -- 12 bytes
    #    Format: !BBBBIHH
    #      protocolVersion (B)  exerciseID (B)  pduType (B)  protocolFamily (B)
    #      timestamp (I)  length (H)  pduStatus+padding (H)
    # ----------------------------------------------------------
    header = struct.pack('!BBBBIHH',
        7,            # protocolVersion  -- DIS 7 (IEEE 1278.1-2012)
        EXERCISE_ID,  # exerciseID       -- server filter (must == config)
        2,            # pduType          -- FirePdu = 2
        2,            # protocolFamily   -- Warfare Family = 2
        timestamp,    # timestamp        -- 32-bit unsigned big-endian
        PDU_LENGTH,   # length           -- total PDU size in bytes
        0,            # pduStatus + padding = 0
    )  # -> 12 bytes

    # ----------------------------------------------------------
    # 2. WarfareFamilyPdu -- 12 bytes
    #    firingEntityID (HHH) + targetEntityID (HHH)
    # ----------------------------------------------------------
    warfare = struct.pack('!HHHHHH',
        FIRING_SITE, FIRING_APP, FIRING_ENTITY,  # firingEntityID:  site / app / entity
        TARGET_SITE, TARGET_APP, TARGET_ENTITY,  # targetEntityID:  site / app / entity
    )  # -> 12 bytes

    # ----------------------------------------------------------
    # 3. munitionExpendableID (EntityID) -- 6 bytes
    #    siteID (H)  applicationID (H)  entityID (H)
    # ----------------------------------------------------------
    munition_expendable_id = struct.pack('!HHH',
        MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY,
    )  # -> 6 bytes

    # ----------------------------------------------------------
    # 5. eventID (EventIdentifier) -- 6 bytes
    #    simulationAddress.siteID (H)  simulationAddress.applicationID (H)
    #    eventNumber (H)
    # ----------------------------------------------------------
    event_id = struct.pack('!HHH',
        EVENT_SITE, EVENT_APP, event_number,
    )  # -> 6 bytes

    # ----------------------------------------------------------
    # 6. fireMissionIndex -- 4 bytes
    #    Unsigned int -- event_number used as index
    # ----------------------------------------------------------
    fire_mission_index = struct.pack('!I', event_number)  # -> 4 bytes

    # ----------------------------------------------------------
    # 7. locationInWorldCoordinates (Vector3Double) -- 24 bytes
    #    x (d)  y (d)  z (d)  -- ECEF coordinates in meters
    # ----------------------------------------------------------
    location = struct.pack('!ddd',
        LOCATION_X,
        LOCATION_Y,
        LOCATION_Z,
    )  # -> 24 bytes

    # ----------------------------------------------------------
    # 8. MunitionDescriptor -- 16 bytes
    #    munitionType / EntityType (8 bytes):
    #      entityKind (B)  domain (B)  country (H)
    #      category (B)  subcategory (B)  specific (B)  extra (B)
    #    warhead (H)  fuse (H)  quantity (H)  rate (H)
    # ----------------------------------------------------------
    descriptor = struct.pack('!BBHBBBBHHHH',
        ENTITY_KIND, DOMAIN, COUNTRY,               # EntityType -- munition identity
        CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA,     # EntityType -- details
        WARHEAD, FUSE, QUANTITY, RATE,              # firing parameters
    )  # -> 16 bytes

    # ----------------------------------------------------------
    # 9. velocity (Vector3Float) -- 12 bytes
    #    x (f)  y (f)  z (f)  -- velocity vector in m/s
    # ----------------------------------------------------------
    velocity = struct.pack('!fff',
        VEL_X, VEL_Y, VEL_Z,
    )  # -> 12 bytes

    # ----------------------------------------------------------
    # 10. range (float) -- 4 bytes
    #     Firing distance in meters
    # ----------------------------------------------------------
    range_field = struct.pack('!f', RANGE)  # -> 4 bytes

    # --- Assemble the full packet ---
    packet = (
          header                # 12
        + warfare               # 12
        + munition_expendable_id #  6
        + event_id              #  6
        + fire_mission_index    #  4
        + location              # 24
        + descriptor            # 16
        + velocity              # 12
        + range_field           #  4
    )                           # = 96 bytes

    # --- Integrity checks ---
    assert len(packet) == PDU_LENGTH, (
        f"Incorrect PDU size: {len(packet)} bytes (expected {PDU_LENGTH})"
    )
    assert packet[2] == 2, (
        f"Incorrect pduType: {packet[2]} (expected 2 = FirePdu)"
    )

    return packet


# ============================================================
# SEND LOOP
# ============================================================

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Base timestamp: Unix seconds truncated to 32 bits (DIS convention)
    base_timestamp = int(time.time()) & 0xFFFFFFFF

    print("=" * 60)
    print(f"  DIS 7 FirePdu Simulator")
    print(f"  Destination  : {HOST}:{PORT}")
    print(f"  exerciseID   : {EXERCISE_ID}")
    print(f"  PDU size     : {PDU_LENGTH} bytes")
    print(f"  Messages     : {NUM_MESSAGES}  --  delay: {DELAY_SECONDS}s")
    print("=" * 60)

    for i in range(1, NUM_MESSAGES + 1):
        event_number = i
        timestamp = (base_timestamp + i) & 0xFFFFFFFF

        packet = build_fire_pdu(event_number, timestamp)
        sock.sendto(packet, (HOST, PORT))

        print(
            f"  FirePdu #{i:3d} sent -- "
            f"attacker {FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY} -> "
            f"target {TARGET_SITE}:{TARGET_APP}:{TARGET_ENTITY}, "
            f"event {EVENT_SITE}:{EVENT_APP}:{event_number}"
        )

        if i < NUM_MESSAGES:
            time.sleep(DELAY_SECONDS)

    sock.close()
    print("=" * 60)
    print(f"  Done -- {NUM_MESSAGES} FirePdu sent to {HOST}:{PORT}")
    print("=" * 60)


if __name__ == '__main__':
    main()
