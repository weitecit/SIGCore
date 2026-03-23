## Quick Start

1. Clone or copy this repository
2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the application:
   ```bash
   python main.py
   ```

## Docker

```bash
docker build -f config/Dockerfile -t sigcore_image .
docker run -p 8000:8000 sigcore_image
```

## Project Structure

```
.
├── main.py              # Entry point
├── src/
    ├── tests/           # Test files
    ├── __init__.py
    └── app.py          # Main application logic
├── requirements.txt     # Production dependencies
└── README.md           # This file
```
