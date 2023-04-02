# loop project

this project is for loop take home interview

## Installation

project uses fastapi and postgres.
run below command to install requirements

```bash
pip install -r requirements.txt
```

Instructions:
need to have 3 tables to postgres or add csv files to cache/ dir in project root.
after requesting trigger_report/ endpoint, process will begin and progress will be
visible in terminal. after some time result will be saved to cache directory.

to run the server:
```bash
uvicorn main:app --reload
```

