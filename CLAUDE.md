# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Japanese Open Data Journalism project that focuses on accessing and analyzing Japanese government statistical data, particularly from e-Stat (Japan's government statistics portal). The project uses Python with specialized libraries for Japanese data processing, geospatial analysis, and visualization.

## Development Commands

```bash
# Install dependencies
uv sync

# Run the main application
python main.py

# Add new dependencies
uv add <package-name>
```

## Architecture

The project is structured around:
- `main.py`: Primary application entry point that demonstrates e-Stat API usage
- `pyproject.toml`: Project configuration with Japanese-specific data analysis dependencies
- Environment-based configuration using `.env` file for API keys

## Key Dependencies

**Core Data Processing:**
- `pandas`, `numpy`: Standard data manipulation
- `pandas-estat`: Specialized library for accessing Japanese e-Stat API data
- `matplotlib`, `seaborn`, `plotly`: Visualization libraries
- `japanize-matplotlib`: Japanese font support for matplotlib

**Geospatial Analysis:**
- `geopandas`, `folium`: Geographic data processing and interactive mapping
- `jageocoder`: Japanese address geocoding
- `shapely`, `pyproj`: Geospatial operations and coordinate transformations

**Data Access:**
- `requests`, `beautifulsoup4`: Web scraping and API access
- `python-dotenv`: Environment variable management

## Environment Setup

The application requires an e-Stat API key:
- Create a `.env` file with `ESTAT_APPID=<your_api_key>`
- Obtain API key from https://www.e-stat.go.jp/api/

## Data Sources

Primary focus on Japanese government open data:
- e-Stat API for national statistics (population, economic data, etc.)
- Statistical table IDs used for specific datasets (e.g., 00200521 for census data)
- Geographic data processing for Japanese addresses and locations