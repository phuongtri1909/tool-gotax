import os,cv2
import numpy as np  
import tensorflow.keras.backend as K
from tensorflow.keras.layers import Dense, LSTM, BatchNormalization, Input, Conv2D, MaxPool2D, Lambda, Bidirectional
from tensorflow.keras.models import Model
import tensorflow as tf
from tensorflow.keras.models import load_model
def squeeze1(y):
    return K.squeeze(y, 1)

class CaptchaSolver:
    def __init__(self):
        self.TARGET_HEIGHT = 24
        self.TARGET_WIDTH = 72
        self.CHAR_LIST = list("2345678abcdefghklmnprtwxy")
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.MODEL_PATH = os.path.join(BASE_DIR, "..", "model.keras")
        self.model = load_model(
            self.MODEL_PATH,
            compile=False,
            custom_objects={"squeeze1": squeeze1}
        )
    def solve(self, image_bytes):
        image_array = np.frombuffer(image_bytes, np.uint8)
        original = cv2.imdecode(image_array, cv2.IMREAD_UNCHANGED)
        if original is None:
            print("❌ Could not decode image bytes.")
            return {
                "status": "error",
                "message": "Could not decode image bytes."
            }
        if original.shape[-1] == 4:
            b, g, r, a = cv2.split(original)
            alpha = a / 255.0
            white = np.ones_like(b, dtype=np.uint8) * 255
            blended = cv2.merge([
                (b * alpha + white * (1 - alpha)).astype(np.uint8),
                (g * alpha + white * (1 - alpha)).astype(np.uint8),
                (r * alpha + white * (1 - alpha)).astype(np.uint8)
            ])
            img_gray = cv2.cvtColor(blended, cv2.COLOR_BGR2GRAY)
        else:
            img_gray = cv2.cvtColor(original, cv2.COLOR_BGR2GRAY)

        img_resized = cv2.resize(img_gray, (self.TARGET_WIDTH, self.TARGET_HEIGHT))
        img_input = np.expand_dims(img_resized, axis=-1) / 255.0
        img_input = np.expand_dims(img_input, axis=0)

        prediction = self.model.predict(img_input)
        decoded = K.ctc_decode(
            prediction,
            input_length=np.ones(prediction.shape[0]) * prediction.shape[1],
            greedy=True
        )[0][0]
        out = K.get_value(decoded)

        result = ""
        for p in out[0]:
            if int(p) != -1:
                result += self.CHAR_LIST[int(p)]
        return result