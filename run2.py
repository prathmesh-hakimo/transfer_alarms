import sys
from main2 import main

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <RAW_ALARM_ID>")
        sys.exit(1)
    main(sys.argv[1]) 