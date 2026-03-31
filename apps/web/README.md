# Zanbo Quant NextGen Frontend

## Run dev

```bash
cd /home/zanbo/zanbotest/apps/web
npm install
npm run dev
```

Dev server: `http://127.0.0.1:5173`

## Build

```bash
cd /home/zanbo/zanbotest/apps/web
npm run build
```

Build output: `apps/web/dist`

## Preview build

```bash
cd /home/zanbo/zanbotest/apps/web
npm run preview
```

Preview server: `http://127.0.0.1:4173`

## Admin Token

Protected API actions now require `BACKEND_ADMIN_TOKEN` on the backend.

For browser use, set one of:

```js
localStorage.setItem('zanbo_admin_token', '<BACKEND_ADMIN_TOKEN>')
sessionStorage.setItem('zanbo_admin_token', '<BACKEND_ADMIN_TOKEN>')
```

Or provide `VITE_ADMIN_API_TOKEN` during local development.

## Current modules

- Dashboard
- Stocks List
- Stock Scores
- Stock Detail
- Global News
- CN News
- Stock News
- Daily Summaries
- Signals Overview
- Themes
- Signal Timeline
- Research Reports
- Multi-role Research
- Chatrooms Overview
- Candidate Pool
- Source Monitor
- Database Audit

## Architecture

- Vue 3 + TypeScript + Vite
- Vue Router
- Pinia
- TanStack Query
- Tailwind CSS v4
- Axios
- Markdown rendering
- WebSocket realtime bus
