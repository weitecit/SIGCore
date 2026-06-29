## Quick Start

1. Clone or copy this repository
2. Install Python 3.12
3. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Run the application:
   ```bash
   python main.py
   ```

Para que los notebooks accedan al resto de scripts:
1. Crear un archivo local_paths.pth en el directorio Lib/site-packages del environment.
2. Añadir la ruta absoluta a la carpeta src

o escribe el siguiente comando en el power shell:
```powershell
"$PWD\src" | Out-File "$env:VIRTUAL_ENV\Lib\site-packages\repo_src.pth" -Encoding ascii
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
    └── api.py          # Main application logic
├── requirements.txt     # Production dependencies
└── README.md           # This file
```
