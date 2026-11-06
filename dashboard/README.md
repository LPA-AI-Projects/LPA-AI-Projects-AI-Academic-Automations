# Outline AI Cost Console

Frontend-only dashboard for the course-outline generator's Anthropic costs. Nothing is sent anywhere:
all inputs and imported logs stay in your browser (localStorage).

- **Cost Analysis** - cost per outline / refine, monthly spend, a prompt-caching switch that recalculates
  every number, retry and fallback waste, break-even cache hit rate, and an all-costs ledger (CSV export).
- **Price Comparison** - compare models on the same workload, with caching on or off.
- **Requests** - every outline request from the live database: source (Zoho or Bitrix), course, company, status,
  refinements and cost. Click a row for a slide-over with all inputs, the refinement history (feedback, dates, PDFs),
  the responsible person (looked up live from the Bitrix task or Zoho record when available) and the cost.
- **Total** - all-time totals across months and sources, with monthly charts, a month-by-month table and CSV export.
- **Month filter** (top of Cost Analysis and Requests) - shows one month's real request counts and spend.
- **Data & Settings** - paste or upload Railway logs; the app reads the lines written by the backend:
  `Anthropic usage | label=outline_json model=claude-sonnet-4-6 input=300 output=5200 cache_write=4900 cache_read=0 web_searches=0 stop=end_turn structured=False`
  and can use the real averages instead of the estimates. Also edits prices, token profile, volumes, fixed costs.

Prices and token counts are editable assumptions. Verify prices on each provider's pricing page; models whose
price is blank are ignored until you enter one.

## Live data

Requests, refinements and months come from the backend's `/api/v1/cost-dashboard/*` endpoints, which read the
database. They need the backend API key (`API_SECRET_KEY`): open Data & Settings (or Requests) and enter it. It is
kept in the browser tab only. Token usage is recorded per call (table `llm_usage`) from the deploy that added it;
requests before that are estimated from the call profile and shown with `~`.

For local development `npm run dev` proxies `/api` to `http://localhost:8000` (override with `VITE_API_PROXY`).

## Run

```bash
cd dashboard
npm install
npm run dev        # http://localhost:5173/cost-dashboard/
npm test           # cost-rule unit tests
npm run build      # type-check + production build into dist/
```

The backend serves the production build at `/cost-dashboard` (see `app/api/cost_dashboard.py`; the Dockerfile
builds it in a Node stage). The app is a client-side single-page app mounted under that base path.

The cost formulas live in `src/lib/cost/calc.ts` (each page has a "How this is calculated" section).
