#!/usr/bin/env python3
"""
Simple, minimal analyzer implementation for testing.
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join("logs", f"simple_analyzer_{datetime.now().strftime('%Y%m%d')}.log"))
    ]
)

log = logging.getLogger("simple_analyzer")

async def analyze_and_generate(cfg, bundle_id=None):
    """
    Simple mock implementation of the analyzer.
    
    Returns a minimal valid result with the bundle ID.
    """
    log.info(f"Using simple analyzer with bundle_id: {bundle_id}")
    
    if not bundle_id:
        # Find the latest bundle
        data_dir = Path("pacific_data")
        if not data_dir.exists():
            log.error("Data directory not found")
            return None
            
        # Read latest_bundle.txt
        try:
            with open(data_dir / "latest_bundle.txt", "r") as f:
                bundle_id = f.read().strip()
                log.info(f"Using latest bundle: {bundle_id}")
        except:
            # Find the most recent bundle
            bundles = [p for p in data_dir.glob("*_*") if p.is_dir()]
            if not bundles:
                log.error("No bundles found")
                return None
                
            # Sort by timestamp (assuming bundle ID ends with timestamp)
            bundles.sort(key=lambda p: p.name.split("_")[-1], reverse=True)
            bundle_id = bundles[0].name
            log.info(f"Using most recent bundle: {bundle_id}")
    
    # Create forecasts directory if it doesn't exist
    forecasts_dir = Path("forecasts")
    forecasts_dir.mkdir(exist_ok=True)
    
    # Generate a minimal forecast
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Create simple markdown forecast
    markdown_content = f"""# Surf Forecast for O'ahu
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} HST
Bundle: {bundle_id}

## North Shore (Country)
- Today: 3-5 ft, light variable winds
- Tomorrow: 4-6 ft, NE winds 5-10 kts
- 3-Day Outlook: Holding at 4-6 ft

## South Shore (Town)
- Today: 1-3 ft, light onshore winds
- Tomorrow: 1-2 ft, light trades
- 3-Day Outlook: Small background swell

## Extended Forecast
The North Pacific remains active with a medium-sized storm developing off Japan.
Expect north swell to arrive in 5-7 days.
"""

    # Create HTML version
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>O'ahu Surf Forecast</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1, h2 {{ color: #0066cc; }}
    </style>
</head>
<body>
    <h1>Surf Forecast for O'ahu</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} HST</p>
    <p>Bundle: {bundle_id}</p>
    
    <h2>North Shore (Country)</h2>
    <ul>
        <li>Today: 3-5 ft, light variable winds</li>
        <li>Tomorrow: 4-6 ft, NE winds 5-10 kts</li>
        <li>3-Day Outlook: Holding at 4-6 ft</li>
    </ul>
    
    <h2>South Shore (Town)</h2>
    <ul>
        <li>Today: 1-3 ft, light onshore winds</li>
        <li>Tomorrow: 1-2 ft, light trades</li>
        <li>3-Day Outlook: Small background swell</li>
    </ul>
    
    <h2>Extended Forecast</h2>
    <p>The North Pacific remains active with a medium-sized storm developing off Japan.
    Expect north swell to arrive in 5-7 days.</p>
</body>
</html>
"""

    # Write forecast files
    md_path = forecasts_dir / f"forecast_{timestamp}.md"
    html_path = forecasts_dir / f"forecast_{timestamp}.html"
    
    with open(md_path, "w") as f:
        f.write(markdown_content)
    
    with open(html_path, "w") as f:
        f.write(html_content)
    
    log.info(f"Generated forecast files: {md_path} and {html_path}")
    
    # Return result with paths
    return {
        "markdown": str(md_path),
        "html": str(html_path),
        "bundle_id": bundle_id,
        "timestamp": timestamp,
        "run_id": timestamp
    }

async def main():
    """Test the simple analyzer directly."""
    import sys
    bundle_id = sys.argv[1] if len(sys.argv) > 1 else None
    result = await analyze_and_generate(None, bundle_id)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(main())