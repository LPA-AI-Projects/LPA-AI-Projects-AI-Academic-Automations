# Outline AI Cost Console

Frontend-only dashboard for the course-outline generator's Anthropic costs. Nothing is sent anywhere:
all inputs and imported logs stay in your browser (localStorage).

- **Cost Analysis** - cost per outline / refine, monthly spend, a prompt-caching switch that recalculates
  every number, retry and fallback waste, break-even cache hit rate, and an all-costs ledger (CSV export).
- **Price Comparison** - compare models on the same workload, with caching on or off.
- **Data & Settings** - paste or upload Railway logs; the app reads the lines written by the backend:
  `Anthropic usage | label=outline_json model=claude-sonnet-4-6 input=300 output=5200 cache_write=4900 cache_read=0 web_searches=0 stop=end_turn structured=False`
  and can use the real averages instead of the estimates. Also edits prices, token profile, volumes, fixed costs.

Prices and token counts are editable assumptions. Verify prices on each provider's pricing page; models whose
price is blank are ignored until you enter one.

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
