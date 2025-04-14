# notion-fitnotes

Create a notion integration.

Create the following databases and share them with the integration:
- Bodyweight: `sql_id` (number), `Date` (date), `Value` (number)
- Exercises: `sql_id` (number), `Name` (title)
- Workouts: `sql_id` (number), `Exercise` (relation to `Exercises`), `Date` (date), `Weight` (number), `Reps` (number)

Create a .env file:

```
SLS_ORG=
NOTION_API_KEY=secret...
NOTION_BODYWEIGHT_DATABASE_ID=
NOTION_EXERCISE_DATABASE_ID=
NOTION_WORKOUT_DATABASE_ID=
```

Install requirements with python 3.13:

```bash
python -m venv .venv
. venv/bin/activate
pip install -r requirements.txt
```

Install serverless deps:

```bash
npm i
```

Deploy with `serverless`:

```bash
sls deploy
```
