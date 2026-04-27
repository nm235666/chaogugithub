# Core Business Flow

This project keeps the existing data assets and UI shell, but the first product promise is a small simulated investment workflow. Pages outside this workflow are supporting tools until the loop below is real.

## First Real Loop

```text
candidate -> evidence packet -> human decision -> executable order
-> simulated execution/cancel -> position update -> review task -> rule correction
```

The first loop is simulated trading only. It does not place real broker orders.

## Product Surface Classes

### Real Closed Loop

These pages must be backed by real backend state and should not show completed states unless the backend has written them:

- Decision board and decision actions.
- Portfolio orders.
- Portfolio positions.
- Portfolio reviews.

### Research Support

These pages provide evidence for human decisions. They may summarize or score information, but they do not imply an executable trade by themselves:

- News and stock news.
- Signals and signal graphs.
- Chatroom sentiment and candidate pools.
- Scoreboards.
- Multi-role analysis and roundtable output.

### Experiment / Operations

These pages are useful for operations or research experiments, but they are not part of the first product promise:

- Agent operations and governance.
- System monitors and job operations.
- Quant factor experiments.
- LLM provider administration.

## Backend Contract

- `watch` and `defer` are observation actions. They may create audit records, but they are not executable orders.
- `buy`, `add`, `sell`, `reduce`, and `close` are trading actions. They must carry a positive `size` before they can become executable.
- An order moving to `executed` must update `portfolio_positions`.
- Executed orders must create a pending review record so the review chain cannot silently stop.
- Active positions are rows in `portfolio_positions` with `quantity > 0`.

## Non-goals

- No real broker integration in this phase.
- No new pages before the loop above is true.
- No promise that LLM output is a trading engine.
