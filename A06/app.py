import os
import uvicorn
from fastapi import FastAPI, File, UploadFile
from keras.models import load_model
from tensorflow.keras.models import Sequential
import numpy as np
from PIL import Image
import argparse

app = FastAPI()

# Setting environment variable to ensure consistent results from floating-point round-off errors
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# Parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--model_path', type=str, required=True, help='Path to the saved MNIST model')
args = parser.parse_args()

# Loading the Keras model from the specified path
def load_keras_model(path: str):
    try:
        return load_model(path)
    except ValueError:
        raise Exception('Keras model not found at the given path, exiting...')

# Predicting the digit from the model
def predict_digit(model, data_point):
    data_point = np.array(data_point).reshape(1, 784)
    prediction = model.predict(data_point)
    digit = str(np.argmax(prediction))
    return digit

# Preprocessing the uploaded image
def preprocess_image(file):
    image = Image.open(file.file)
    # Convert to grayscale and resize
    image = image.convert('L').resize((28, 28))
    data_point = list(image.getdata())
    return data_point

# API endpoint for digit prediction
@app.post('/predict')
async def predict(file: UploadFile = File(...)):
    # Preprocess the image
    data_point = preprocess_image(file)
    # Load the model
    model = load_keras_model(args.model_path)
    model.compile(loss='categorical_crossentropy', metrics=['accuracy'])
    # Predict the digit
    digit = predict_digit(model, data_point)
    return {"digit": digit}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)