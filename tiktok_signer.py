import base64
import hashlib
import random
import time
import urllib.parse
import httpx

USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

class XBogus:
    __string = "Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe="
    __array = [None]*48 + list(range(10)) + [None]*39 + list(range(10, 16))
    __canvas = 3873194319

    @staticmethod
    def disturb_array(a, b, e, d, c, f, t, n, o, i, r, _, x, u, s, l, v, h, g):
        arr = [0] * 19
        arr[0], arr[10], arr[1], arr[11], arr[2], arr[12] = a, b, e, d, c, f
        arr[3], arr[13], arr[4], arr[14], arr[5], arr[15] = t, n, o, i, r, _
        arr[6], arr[16], arr[7], arr[17], arr[8], arr[18], arr[9] = x, u, s, l, v, h, g
        return arr

    @staticmethod
    def generate_garbled_1(a, b, e, d, c, f, t, n, o, i, r, _, x, u, s, l, v, h, g):
        arr = [0] * 19
        arr[0], arr[1], arr[2], arr[3], arr[4], arr[5] = a, r, b, _, e, x
        arr[6], arr[7], arr[8], arr[9], arr[10], arr[11] = d, u, c, s, f, l
        arr[12], arr[13], arr[14], arr[15], arr[16], arr[17], arr[18] = t, v, n, h, o, g, i
        return "".join(map(chr, map(int, arr)))

    @staticmethod
    def generate_num(text):
        return [ord(text[i]) << 16 | ord(text[i + 1]) << 8 | ord(text[i + 2]) for i in range(0, len(text) - 2, 3)]

    @staticmethod
    def generate_garbled_2(a, b, c):
        return chr(a) + chr(b) + c

    @staticmethod
    def generate_garbled_3(a, b):
        d = list(range(256))
        c = 0
        f = ""
        for i in range(256):
            c = (c + d[i] + ord(a[i % len(a)])) % 256
            d[i], d[c] = d[c], d[i]
        t = c = 0
        for i in range(len(b)):
            t = (t + 1) % 256
            c = (c + d[t]) % 256
            d[t], d[c] = d[c], d[t]
            f += chr(ord(b[i]) ^ d[(d[t] + d[c]) % 256])
        return f

    def calculate_md5(self, input_string):
        array = self.md5_to_array(input_string) if isinstance(input_string, str) else input_string
        return hashlib.md5(bytes(array)).hexdigest()

    def md5_to_array(self, md5_str):
        md5_str = str(md5_str).lower()
        if len(md5_str) > 32:
            return [ord(char) for char in md5_str]
        res = []
        for i in range(0, len(md5_str), 2):
            c1 = ord(md5_str[i])
            c2 = ord(md5_str[i+1]) if i+1 < len(md5_str) else 48
            v1 = self.__array[c1] if c1 < len(self.__array) and self.__array[c1] is not None else 0
            v2 = self.__array[c2] if c2 < len(self.__array) and self.__array[c2] is not None else 0
            res.append((v1 << 4) | v2)
        return res

    def process_url_path(self, url_path):
        return self.md5_to_array(self.calculate_md5(self.md5_to_array(self.calculate_md5(url_path))))

    def generate_str(self, num):
        string = [num & 16515072, num & 258048, num & 4032, num & 63]
        string = [i >> j for i, j in zip(string, range(18, -1, -6))]
        return "".join([self.__string[i] for i in string])

    @staticmethod
    def handle_ua(a, b):
        d = list(range(256))
        c = 0
        result = bytearray(len(b))
        for i in range(256):
            c = (c + d[i] + ord(a[i % len(a)])) % 256
            d[i], d[c] = d[c], d[i]
        t = c = 0
        for i in range(len(b)):
            t = (t + 1) % 256
            c = (c + d[t]) % 256
            d[t], d[c] = d[c], d[t]
            result[i] = b[i] ^ d[(d[t] + d[c]) % 256]
        return result

    def generate_ua_array(self, user_agent: str, params: int) -> list:
        ua_key = ["\u0000", "\u0001", chr(params)]
        value = self.handle_ua(ua_key, user_agent.encode("utf-8"))
        value = base64.b64encode(value)
        return list(hashlib.md5(value).digest())

    def get_x_bogus(self, query, data=None, method=None, user_agent=USERAGENT, test_time=None):
        timestamp = int(test_time or time.time())
        query_str = urllib.parse.urlencode(query, quote_via=urllib.parse.quote) if isinstance(query, dict) else query
        query_arr = self.process_url_path(query_str)
        ua_array = self.generate_ua_array(user_agent, 8)
        array = [
            64, 0.00390625, 1, 8, query_arr[-2], query_arr[-1], 69, 63,
            ua_array[-2], ua_array[-1],
            timestamp >> 24 & 255, timestamp >> 16 & 255, timestamp >> 8 & 255, timestamp >> 0 & 255,
            self.__canvas >> 24 & 255, self.__canvas >> 16 & 255, self.__canvas >> 8 & 255, self.__canvas >> 0 & 255,
            None
        ]
        zero = 0
        for i in array[:-1]:
            zero ^= int(i) if isinstance(i, float) else i
        array[-1] = zero
        garbled = self.generate_garbled_1(*self.disturb_array(*array))
        garbled = self.generate_garbled_2(2, 255, self.generate_garbled_3("ÿ", garbled))
        return "".join(self.generate_str(i) for i in self.generate_num(garbled))

class XGnarly:
    _AA = [
        0xFFFFFFFF, 138, 1498001188, 211147047, 253, None, 203, 288, 9, 1196819126,
        3212677781, 135, 263, 193, 58, 18, 244, 2931180889, 240, 173, 268, 2157053261,
        261, 175, 14, 5, 171, 270, 156, 258, 13, 15, 3732962506, 185, 169, 2, 6, 132,
        162, 200, 3, 160, 217618912, 62, 2517678443, 44, 164, 4, 96, 183, 2903579748,
        3863347763, 119, 181, 10, 190, 8, 2654435769, 259, 104, 230, 128, 2633865432,
        225, 1, 257, 143, 179, 16, 600974999, 185100057, 32, 188, 53, 2718276124,
        177, 196, 4294967296, 147, 117, 17, 49, 7, 28, 12, 266, 216, 11, 0, 45, 166, 247, 1451689750
    ]
    _OT = [_AA[9], _AA[69], _AA[51], _AA[92]]
    _MASK32 = 0xFFFFFFFF
    _BASE64_ALPHABET = "u09tbS3UvgDEe6r-ZVMXzLpsAohTn7mdINQlW412GqBjfYiyk8JORCF5/xKHwacP="

    def __init__(self):
        now_ms = int(time.time() * 1000)
        self.kt = [
            self._AA[44], self._AA[74], self._AA[10], self._AA[62], self._AA[42], self._AA[17],
            self._AA[2], self._AA[21], self._AA[3], self._AA[70], self._AA[50], self._AA[32],
            self._AA[0] & now_ms, random.randint(0, self._AA[77]), random.randint(0, self._AA[77]),
            random.randint(0, self._AA[77])
        ]
        self.St = self._AA[88]

    @classmethod
    def _u32(cls, x: int) -> int: return x & cls._MASK32

    @classmethod
    def _rotl(cls, x: int, n: int) -> int: return cls._u32(((x << n) & cls._MASK32) | (x >> (32 - n)))

    @classmethod
    def _quarter(cls, st: list, a: int, b: int, c: int, d: int):
        st[a] = cls._u32(st[a] + st[b])
        st[d] = cls._rotl(st[d] ^ st[a], 16)
        st[c] = cls._u32(st[c] + st[d])
        st[b] = cls._rotl(st[b] ^ st[c], 12)
        st[a] = cls._u32(st[a] + st[b])
        st[d] = cls._rotl(st[d] ^ st[a], 8)
        st[c] = cls._u32(st[c] + st[d])
        st[b] = cls._rotl(st[b] ^ st[c], 7)

    @classmethod
    def _chacha_block(cls, state: list, rounds: int) -> list:
        w = state.copy()
        r = 0
        while r < rounds:
            cls._quarter(w, 0, 4, 8, 12); cls._quarter(w, 1, 5, 9, 13)
            cls._quarter(w, 2, 6, 10, 14); cls._quarter(w, 3, 7, 11, 15)
            r += 1
            if r >= rounds: break
            cls._quarter(w, 0, 5, 10, 15); cls._quarter(w, 1, 6, 11, 12)
            cls._quarter(w, 2, 7, 12, 13); cls._quarter(w, 3, 4, 13, 14)
            r += 1
        for i in range(16): w[i] = cls._u32(w[i] + state[i])
        return w

    def rand(self) -> float:
        e = self._chacha_block(self.kt, 8)
        t = e[self.St]
        r = (e[self.St + 8] & 0xFFFFFFF0) >> 11
        if self.St == 7:
            self.kt[12] = self._u32(self.kt[12] + 1)
            self.St = 0
        else:
            self.St += 1
        return (t + 4294967296 * r) / (2**53)

    @staticmethod
    def _num_to_bytes(val: int) -> list:
        return [(val >> 8) & 0xFF, val & 0xFF] if val < 65535 else [(val >> 24) & 0xFF, (val >> 16) & 0xFF, (val >> 8) & 0xFF, val & 0xFF]

    @staticmethod
    def _be_int_from_str(s: str) -> int:
        b = s.encode("utf-8")[:4]
        acc = 0
        for x in b: acc = (acc << 8) | x
        return acc & XGnarly._MASK32

    def _encrypt_chacha(self, key12_words: list, rounds: int, data: list):
        state = self._OT + key12_words
        n_full, leftover = len(data) // 4, len(data) % 4
        words = [0] * ((len(data) + 3) // 4)
        for i in range(n_full):
            j = 4 * i
            words[i] = data[j] | (data[j + 1] << 8) | (data[j + 2] << 16) | (data[j + 3] << 24)
        if leftover:
            v = 0
            base = 4 * n_full
            for c in range(leftover): v |= data[base + c] << (8 * c)
            words[n_full] = v
        o = 0
        st = state.copy()
        while o + 16 < len(words):
            stream = self._chacha_block(st, rounds)
            st[12] = self._u32(st[12] + 1)
            for k in range(16): words[o + k] ^= stream[k]
            o += 16
        if o < len(words):
            stream = self._chacha_block(st, rounds)
            for k in range(len(words) - o): words[o + k] ^= stream[k]
        for i in range(n_full):
            w = words[i]
            j = 4 * i
            data[j:j+4] = [w & 0xFF, (w >> 8) & 0xFF, (w >> 16) & 0xFF, (w >> 24) & 0xFF]
        if leftover:
            w = words[n_full]
            base = 4 * n_full
            for c in range(leftover): data[base + c] = (w >> (8 * c)) & 0xFF

    def generate(self, query_string: str, body: str = "", user_agent: str = USERAGENT, envcode: int = 0, version: str = "5.1.1") -> str:
        timestamp_ms = int(time.time() * 1000)
        obj = {
            1: 1, 2: envcode,
            3: hashlib.md5(query_string.encode()).hexdigest(),
            4: hashlib.md5((body or "").encode()).hexdigest(),
            5: hashlib.md5(user_agent.encode()).hexdigest(),
            6: timestamp_ms // 1000, 7: 1508145731,
            8: int((timestamp_ms * 1000) % 2147483648), 9: version,
            10: "1.0.0.314", 11: 1
        }
        v12 = 0
        for i in range(1, 12):
            v = obj[i]
            to_xor = v if isinstance(v, int) else self._be_int_from_str(v)
            v12 ^= to_xor
        obj[12] = v12 & self._MASK32
        v0 = 0
        for i in range(1, len(obj) + 1):
            v = obj[i]
            if isinstance(v, int): v0 ^= v
        obj[0] = v0 & self._MASK32

        payload = [len(obj)]
        for k, v in obj.items():
            payload.append(k)
            val_bytes = self._num_to_bytes(v) if isinstance(v, int) else list(v.encode("utf-8"))
            payload.extend(self._num_to_bytes(len(val_bytes)))
            payload.extend(val_bytes)
        base_str = "".join(chr(x) for x in payload)

        key_words, key_bytes, round_accum = [], [], 0
        for _ in range(12):
            word = int(self.rand() * 4294967296) & self._MASK32
            key_words.append(word)
            round_accum = (round_accum + (word & 15)) & 15
            key_bytes.extend([word & 0xFF, (word >> 8) & 0xFF, (word >> 16) & 0xFF, (word >> 24) & 0xFF])
        rounds = round_accum + 5
        data = [ord(ch) for ch in base_str]
        self._encrypt_chacha(key_words, rounds, data)
        enc = "".join(chr(x) for x in data)

        insert_pos = 0
        for b in key_bytes: insert_pos = (insert_pos + b) % (len(enc) + 1)
        for ch in enc: insert_pos = (insert_pos + ord(ch)) % (len(enc) + 1)

        key_bytes_str = "".join(chr(b) for b in key_bytes)
        final_str = chr(((1 << 6) ^ (1 << 3) ^ 3) & 0xFF) + enc[:insert_pos] + key_bytes_str + enc[insert_pos:]

        out = []
        full_len = (len(final_str) // 3) * 3
        for i in range(0, full_len, 3):
            block = (ord(final_str[i]) << 16) | (ord(final_str[i + 1]) << 8) | ord(final_str[i + 2])
            out.extend([
                self._BASE64_ALPHABET[(block >> 18) & 63],
                self._BASE64_ALPHABET[(block >> 12) & 63],
                self._BASE64_ALPHABET[(block >> 6) & 63],
                self._BASE64_ALPHABET[block & 63],
            ])
        # Handle leftover bytes (1 or 2) for full base64 spec compliance
        rem = len(final_str) - full_len
        if rem == 1:
            block = ord(final_str[full_len]) << 16
            out.extend([
                self._BASE64_ALPHABET[(block >> 18) & 63],
                self._BASE64_ALPHABET[(block >> 12) & 63],
            ])
        elif rem == 2:
            block = (ord(final_str[full_len]) << 16) | (ord(final_str[full_len + 1]) << 8)
            out.extend([
                self._BASE64_ALPHABET[(block >> 18) & 63],
                self._BASE64_ALPHABET[(block >> 12) & 63],
                self._BASE64_ALPHABET[(block >> 6) & 63],
            ])

        return "".join(out)

def get_signed_tiktok_request(params_dict: dict):
    """Generates headers and appends X-Bogus directly into query params."""
    xg = XGnarly()
    xb = XBogus()
    
    # 1. Generate X-Bogus and append to query parameters
    x_bogus = xb.get_x_bogus(params_dict, user_agent=USERAGENT)
    signed_params = params_dict.copy()
    signed_params["X-Bogus"] = x_bogus

    # 2. Build canonical query string for X-Gnarly signature
    query_str = urllib.parse.urlencode(signed_params)
    x_gnarly = xg.generate(query_str, user_agent=USERAGENT)

    headers = {
        "User-Agent": USERAGENT,
        "Referer": "https://www.tiktok.com/",
        "X-Gnarly": x_gnarly,
    }
    return signed_params, headers