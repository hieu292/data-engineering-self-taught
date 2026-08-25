#!/usr/bin/env python3
"""Tự ký một PAT (Personal Access Token) cho Unity Catalog OSS.

VÌ SAO CẦN SCRIPT NÀY: ảnh unitycatalog/unitycatalog đi kèm một cặp khoá RSA
cố định (etc/conf/private_key.der + key_id.txt) và một token admin BAKED-IN
(etc/conf/token.txt) — nhưng token đó có `iat` cố định từ lúc ảnh được build,
không phải lúc container chạy. server.access-token-timeout kiểm tra
`iat + timeout`, nên một token cũ có thể đã "hết hạn" so với đồng hồ thật dù
cặp khoá ký nó vẫn còn hợp lệ. Ký một token MỚI bằng ĐÚNG khoá đó, `iat` là
bây giờ, xong.

Không cần một Identity Provider ngoài: authorization=enable của UC chấp nhận
bất kỳ JWT nào ký đúng bằng khoá của nó, issuer khớp `server.allowed-issuers`
(xem docker/unity-catalog/conf/server.properties: "internal").

Cần: pip install pyjwt cryptography

Dùng:
    python3 scripts/mint_uc_token.py admin
    python3 scripts/mint_uc_token.py analyst@lakehouse.local
"""
import subprocess
import sys
import time
import uuid

import jwt
from cryptography.hazmat.primitives.serialization import load_der_private_key

IMAGE = "unitycatalog/unitycatalog:v0.6.0"


def extract_from_image(path: str) -> bytes:
    """Đọc một file từ bên trong image UC mà không cần tạo container thường trực."""
    return subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "cat", IMAGE, path],
        check=True,
        capture_output=True,
    ).stdout


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Dùng: {sys.argv[0]} <sub>", file=sys.stderr)
        sys.exit(1)
    sub = sys.argv[1]

    private_key_der = extract_from_image("etc/conf/private_key.der")
    kid = extract_from_image("etc/conf/key_id.txt").decode().strip()
    private_key = load_der_private_key(private_key_der, password=None)

    payload = {
        "sub": sub,
        "iss": "internal",
        "iat": int(time.time()),
        "jti": str(uuid.uuid4()),
        "type": "SERVICE",
    }
    token = jwt.encode(payload, private_key, algorithm="RS512", headers={"kid": kid})
    print(token)


if __name__ == "__main__":
    main()
