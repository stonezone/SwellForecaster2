"""Image utilities for SwellForecaster."""
import logging
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Union, Any

# Set up logger
logger = logging.getLogger("utils.image_utils")


def create_placeholder_image(
    text: str = "No Image Available",
    width: int = 800,
    height: int = 600,
    text_color: Tuple[int, int, int] = (255, 255, 255),
    bg_color: Tuple[int, int, int] = (100, 100, 100),
    filename: Optional[str] = None,
) -> bytes:
    """
    Create a placeholder image with text.

    Args:
        text: Text to display on the image
        width: Image width in pixels
        height: Image height in pixels
        text_color: RGB color tuple for the text
        bg_color: RGB color tuple for the background
        filename: Optional filename to use for context in the placeholder

    Returns:
        Image data as bytes in PNG format
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        
        # Create image
        img = Image.new("RGB", (width, height), color=bg_color)
        draw = ImageDraw.Draw(img)
        
        # Try to load a font, fall back to default
        try:
            font = ImageFont.truetype("Arial", 36)
        except IOError:
            font = ImageFont.load_default()
        
        # Calculate text position for center alignment
        text_width, text_height = draw.textbbox((0, 0), text, font=font)[2:4]
        text_x = (width - text_width) // 2
        text_y = (height - text_height) // 2
        
        # Draw text
        draw.text((text_x, text_y), text, fill=text_color, font=font)
        
        # Save to bytes
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        
        return buffer.getvalue()
    
    except ImportError:
        # If PIL is not available, return a simple 1x1 transparent PNG
        logger.warning("PIL not available, returning minimal placeholder image")
        return b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82'