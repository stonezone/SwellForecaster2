"""SwellForecaster package installation script."""
from setuptools import setup, find_packages

setup(
    name="swellforecaster",
    version="1.0.0",
    description="AI-powered surf forecasting tool for Oʻahu",
    author="SwellForecaster Team",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[
        "aiohttp>=3.8.0",
        "aiofiles>=23.1.0",
        "pydantic>=2.0.0",
        "tenacity>=8.2.0",
        "openai>=1.0.0",
        "pillow>=9.0.0",
        "numpy>=1.20.0",
        "scipy>=1.7.0",
        "matplotlib>=3.4.0",
        "markdown>=3.3.0",
        "weasyprint>=55.0",
    ],
    extras_require={
        "dev": [
            "ruff>=0.1.0",
            "mypy>=1.0.0",
            "pytest>=7.0.0",
            "pytest-asyncio>=0.18.0",
            "pre-commit>=2.15.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "swellforecaster=forecast_tool.run:main",
            "swellcollector=forecast_tool.collector:main",
            "swellanalyzer=forecast_tool.analyzer:main",
        ],
    },
)