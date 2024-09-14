import secrets
import uuid
from datetime import datetime, timezone


def uuid7():
    """Generate a UUIDv7. 36char as a string."""
    #TODO: consider switching this to a class that inherits from uuid.UUID. Especially if we switch to storing as s BLOB(16) in sqlite for performance and space efficiency.
    # Get current Unix timestamp in milliseconds
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Convert timestamp to bytes (48 bits)
    timestamp_bytes = timestamp.to_bytes(6, byteorder='big')
    
    # Generate 74 bits of randomness
    random_bytes = secrets.token_bytes(10)
    
    # Combine timestamp and random bytes
    combined = timestamp_bytes + random_bytes
    
    # Set version (7) and variant bits
    combined = bytearray(combined)
    combined[6] = (combined[6] & 0x0f) | 0x70  # version 7
    combined[8] = (combined[8] & 0x3f) | 0x80  # RFC 4122 variant
    
    return uuid.UUID(bytes=bytes(combined))