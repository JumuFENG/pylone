# Python 3
# -*- coding:utf-8 -*-
import base64

CRYPTO_AVAILABLE = False

try:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    CRYPTO_AVAILABLE = True
except Exception:
    pass

if not CRYPTO_AVAILABLE:
    try:
        import pyaes
        PYAES_AVAILABLE = True
    except Exception:
        PYAES_AVAILABLE = False


class AesCBCBase64:
    ''' AES 加解密 Mode: CBC, padding: pkcs7. Base64
    '''
    def __init__(self, key, iv):
        """
        初始化AES加解密对象

        Args:
            key (str): 密钥
            iv (str): 初始化向量
        """
        self.key = key.encode()
        self.iv = iv.encode()

    def pkcs7_padding(self, data):
        """
        对数据进行PKCS7填充

        Args:
            data (bytes): 待填充的数据

        Returns:
            bytes: 填充后的数据
        """
        if CRYPTO_AVAILABLE:
            padder = padding.PKCS7(algorithms.AES.block_size).padder()
            padded_data = padder.update(data) + padder.finalize()
            return padded_data
        else:
            padding_length = 16 - (len(data) % 16)
            padding_bytes = bytes([padding_length] * padding_length)
            return data + padding_bytes

    def encrypt(self, data):
        """
        使用AES加密数据

        Args:
            data (str): 待加密的数据

        Returns:
            str: Base64编码的加密数据
        """
        data = data.encode()

        if CRYPTO_AVAILABLE:
            cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv), backend=default_backend())
            encryptor = cipher.encryptor()
            padded_data = encryptor.update(self.pkcs7_padding(data))
            return base64.b64encode(padded_data).decode()
        else:
            padded_data = self.pkcs7_padding(data)
            encrypter = pyaes.Encrypter(pyaes.AESModeOfOperationCBC(self.key, self.iv))
            encrypted = encrypter.feed(padded_data)
            encrypted += encrypter.feed()  # Finalize
            return base64.b64encode(encrypted).decode()

    def pkcs7_unpadding(self, padded_data):
        """
        对PKCS7填充的数据进行解除填充

        Args:
            padded_data (bytes): 填充的数据

        Returns:
            bytes: 解除填充后的数据
        """
        if CRYPTO_AVAILABLE:
            unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
            data = unpadder.update(padded_data)
            try:
                unpadded_data = data + unpadder.finalize()
            except ValueError:
                raise Exception('无效的加密信息!')
            else:
                return unpadded_data
        else:
            padding_length = padded_data[-1]
            if padding_length > 16 or padding_length == 0:
                raise Exception('无效的加密信息!')
            return padded_data[:-padding_length]

    def decrypt(self, data):
        """
        解密Base64编码的加密数据

        Args:
            data (str): Base64编码的加密数据

        Returns:
            str: 解密后的原始数据
        """
        data = base64.b64decode(data)

        if CRYPTO_AVAILABLE:
            cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv), backend=default_backend())
            decryptor = cipher.decryptor()
            unpadded_data = self.pkcs7_unpadding(decryptor.update(data))
            return unpadded_data.decode()
        else:
            decryptor = pyaes.Decrypter(pyaes.AESModeOfOperationCBC(self.key, self.iv))
            decrypted = decryptor.feed(data)
            decrypted += decryptor.feed()  # Finalize
            unpadded_data = self.pkcs7_unpadding(decrypted)
            return unpadded_data.decode()
