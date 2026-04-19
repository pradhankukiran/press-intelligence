# Press Intelligence Frontend

Next.js frontend for Press Intelligence. The UI combines
editorial analytics and admin/ops workflows in one app.

## Run locally

```bash
npm install
npm run dev
```

Open `http://localhost:3000` with the backend running on `http://localhost:8000`.

The frontend expects:

- `NEXT_PUBLIC_API_BASE_URL=http://localhost:8000`

## Checks

```bash
npm run lint
npm run build
```

## App areas

- `Overview`: KPI strip, publishing pulse, top sections
- `Analytics`: section mix, tag momentum, publishing volume, leader table
- `Operations`: DAG health, backfill form, data quality, run history
