from uuid import UUID

import jwt
from fastapi import HTTPException, Request, status


def current_user_id(request: Request) -> UUID:
    """user_id берётся только из JWT (claim `sub`), никогда из тела запроса."""
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    settings = request.app.state.jwt
    try:
        claims = jwt.decode(token, settings.secret, algorithms=[settings.algorithm])
        return UUID(str(claims["sub"]))
    except (jwt.PyJWTError, KeyError, ValueError) as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token") from e
