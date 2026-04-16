from config import MODE
from historical import run_historical
from streaming import run_streaming

if __name__ == "__main__":
    print(f"Starting producer in mode: {MODE}")
    if MODE == "historical":
        run_historical()

    elif MODE == "streaming":
        run_streaming()

    else:
        raise ValueError(f"Unsupported mode: {MODE}")
