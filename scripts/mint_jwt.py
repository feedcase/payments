"""Выпустить dev-JWT: poetry run python scripts/mint_jwt.py [user_uuid]"""

import os
import sys
import uuid

import jwt

secret = os.environ.get("JWT__SECRET", "dev-secret-change-me-32-bytes-minimum")
sub = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
print(jwt.encode({"sub": sub}, secret, algorithm="HS256"))
