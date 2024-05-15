# FastAPI MNIST Digit Recognition API

This API uses a trained Keras model to recognize digits from hand-drawn images. It accepts images in PNG format and returns the predicted digit.

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/Rinkle-S/CS5830/A06.git
   cd fastapi-mnist
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Download the trained MNIST model (`mnist_model.h5`) and place it in the root directory of the project.

4. Start the FastAPI server:

   ```bash
   uvicorn main:app --reload
   ```

5. Visit `http://localhost:8000/docs` in your browser to access the API documentation and try out the digit recognition endpoint.

## API Usage

### Digit Recognition Endpoint

- **URL:** `/predict`
- **Method:** `POST`
- **Request Body:** Upload a PNG image of a hand-drawn digit
- **Response:** JSON object containing the predicted digit

## Example

1. Upload a PNG image of a hand-drawn digit using the API documentation or a tool like Postman.
2. Receive a JSON response with the predicted digit.

## Dependencies

- FastAPI
- TensorFlow
- Keras
- NumPy
- PIL

## Author

Rinkle Sebastian
