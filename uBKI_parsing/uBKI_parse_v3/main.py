import asyncio
import pandas as pd
from orchestrator import UBKIParser
from utils import logger

def read_input_csv(path: str):
    df = pd.read_csv(path, dtype=str)
    return df["IDENTIFYCODE"].dropna().astype(str).tolist()

async def main():
    logger.info("Starting UBKI Parser...")
    edrpous = read_input_csv(r"C:\OTP Draft\YouControl\uBKI_parsing\production\companies.csv")
    parser = UBKIParser(edrpous)
    await parser.run()

if __name__ == "__main__":
    asyncio.run(main())
