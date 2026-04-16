from config import MODE
from historical import run_historical

if __name__ == "__main__":
    print(f"Starting producer in mode: {MODE}")
    if MODE == "historical":
        run_historical()
        
    else: 
        raise ValueError(f"Unsupported mode: {MODE}")