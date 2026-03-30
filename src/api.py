import os
import lightgbm as lgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Azure Telemetry Invocation Predictor")

class InvocationFeatures(BaseModel):
    total_invocations: float
    active_minutes: float
    days_active: float
    max_invocations: float
    recent_invocations: float
    early_invocations: float
    trend_ratio: float
    avg_inv_per_min: float
    activity_density: float

model = None

@app.on_event("startup")
def load_model():
    global model
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(base_dir, "..", "models", "lgbm_invocation_model.txt")
    
    if os.path.exists(model_path):
        model = lgb.Booster(model_file=model_path)
        print(f"Model loaded successfully from {model_path}")
    else:
        print(f"Warning: Model not found at {model_path}")

@app.post("/predict")
def predict_invocation(features: InvocationFeatures):
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded.")
    
    feature_values = [[
        features.total_invocations,
        features.active_minutes,
        features.days_active,
        features.max_invocations,
        features.recent_invocations,
        features.early_invocations,
        features.trend_ratio,
        features.avg_inv_per_min,
        features.activity_density
    ]]
    
    prediction_prob = model.predict(feature_values)[0]
    
    return {
        "probability": float(prediction_prob),
        "will_be_invoked": bool(prediction_prob >= 0.5)
    }
