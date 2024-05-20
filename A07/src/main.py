from fastapi import FastAPI, File, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
import io
import PIL
import PIL.Image
import PIL.ImageOps  
import uvicorn
import numpy as np
from tensorflow.keras.models import Sequential
import pickle
import time

from prometheus_client import Counter, Gauge, generate_latest

# Initialize counters and gauges
api_usage_counter = Counter('api_usage', 'API usage counter', ['client_ip'])
processing_time_gauge = Gauge('processing_time', 'Processing time gauge', ['client_ip', 'input_length', 'processing_time_per_character'])

pickle_file = "src/model2.pkl"
with open(pickle_file, "rb") as f:
    model = pickle.load(f)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_image(image: PIL.Image.Image) -> np.ndarray:
    """
    Formats the uploaded image to a 28x28 grayscale image and creates a serialized array of 784 elements.

    Parameters:
    - image (PIL.Image.Image): The uploaded image.

    Returns:
    - np.ndarray: The formatted image as a serialized array of 784 elements.
    """
    image = image.convert("L")
    image = 255 - np.array(image)
    image = np.array(image)
    padded_image = np.pad(image, ((1, 1), (8, 8)), mode='constant', constant_values=0)
    print(padded_image.shape)
    new_array = []
    for i in range(28):
        row = []
        for j in range(28):
            row.append(np.max(padded_image[7*i:7*i+7, 7*j:7*j+7]))
        new_array.append(row)
    new_array = np.array(new_array)
    new_array = np.where(new_array > 40, 255, 0)
    return new_array

def load_model(path: str) -> Sequential:
    with open(path, "rb") as f:
        model = pickle.load(f)
    return model

def predict_digit(model: Sequential, image: list) -> str:
    return str(int(np.argmax(model.predict(np.array(image).reshape(1, 784)))))

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.post("/predict-task-2/")
async def predict(request: Request, file: UploadFile = File(...)):
    client_ip = request.client.host
    api_usage_counter.labels(client_ip).inc()

    start_time = time.time()
    contents = await file.read()
    input_length = len(contents)
    pil_image = PIL.Image.open(io.BytesIO(contents))
    image_array = format_image(pil_image)
    model = load_model("src/model2.pkl")
    digit = predict_digit(model, image_array)
    end_time = time.time()

    total_time = (end_time - start_time) * 1000  # in milliseconds
    processing_time_per_character = (total_time / input_length) * 1000  # in microseconds per character

    processing_time_gauge.labels(client_ip, input_length, processing_time_per_character).set(total_time)

    return {"digit": digit}

@app.get("/metrics")
async def metrics():
    return generate_latest()

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
