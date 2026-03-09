"""
加解密工具模块
提供文件 AES-256-CBC 解密功能
"""

from __future__ import annotations

import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


def decrypt_file(encrypted_data: bytes, aes_key: str) -> bytes:
    """
    使用 AES-256-CBC 解密文件

    :param encrypted_data: 加密的文件数据
    :param aes_key: Base64 编码的 AES-256 密钥
    :returns: 解密后的文件字节数据
    """
    if not encrypted_data:
        raise ValueError("decrypt_file: encrypted_data is empty or not provided")

    if not aes_key or not isinstance(aes_key, str):
        raise ValueError("decrypt_file: aes_key must be a non-empty string")

    # 将 Base64 编码的 aes_key 解码（补齐可能缺失的 padding）
    padded = aes_key + "=" * (-len(aes_key) % 4)
    key = base64.b64decode(padded)

    # IV 取 key 的前 16 字节
    iv = key[:16]

    try:
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(encrypted_data) + decryptor.finalize()

        # 手动去除 PKCS#7 填充（支持 32 字节 block）
        pad_len = decrypted[-1]
        if pad_len < 1 or pad_len > 32 or pad_len > len(decrypted):
            raise ValueError(f"Invalid PKCS#7 padding value: {pad_len}")

        # 验证所有 padding 字节是否一致
        for i in range(len(decrypted) - pad_len, len(decrypted)):
            if decrypted[i] != pad_len:
                raise ValueError("Invalid PKCS#7 padding: padding bytes mismatch")

        return decrypted[: len(decrypted) - pad_len]
    except Exception as e:
        raise ValueError(
            f"decrypt_file: Decryption failed - {e}. "
            "This may indicate corrupted data or an incorrect aes_key."
        ) from e
