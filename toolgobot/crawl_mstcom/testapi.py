import requests
import random, string

def gen_r():
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(5))
for i in range(6):
    print(gen_r())