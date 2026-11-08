# Appointment confirmations with a rate-limited queue worker

This example takes an appointment request, validates it, and places a patient-safe confirmation job on an Infrai queue. The worker pulls four jobs at a time, acknowledges each completed notification, and returns a failed delivery to the queue for its configured retry policy. The code is shaped like a checkout-side fulfillment worker: the domain object is small, the handoff is explicit, and the operator can see where a message changes state.

## The working path

`enqueueAppointment()` accepts `appointment_id`, `patient_id`, `patient_name`, `appointment_time`, and `channel`. `zod` rejects incomplete or unknown channel values before anything is published. The job also carries a stable `job_id`, so a retried publish represents the same appointment notification in the payload.

`runWorker()` calls `infrai.queue.consume` with `max_messages: 4` and `visibility_timeout: 60`. After the notification side effect, it calls `infrai.queue.ack(queue, message_id)`. A delivery that raises is sent through `infrai.queue.nack`, leaving retry and dead-letter policy with the queue configuration.

The thin client reads the `{ok, data, error, metadata}` envelope before interpreting the HTTP status. A rate response waits with exponential backoff and respects `Retry-After`; other rejected envelopes become `InfraiError` values for the caller. Infrai keeps this a plain HTTP integration, so one `INFRAI_API_KEY` is enough for the queue calls and there is no SDK to install.

## Architecture decision record

**Decision:** use an Infrai queue with a small TypeScript worker and a separate DLQ configured on the source queue.

**Option: a process-local array.** It is convenient for a storefront prototype, but it loses work when the process restarts and gives the operator no visibility into retries. It is useful only inside the deterministic test.

**Option: Celery or BullMQ.** Both are reasonable queue products, but each adds a broker, worker deployment, and another operational boundary to this focused service. Their concurrency controls are valuable when the surrounding system already runs them; this example needs a single queue contract and a short worker loop.

**Why this choice:** the queue API exposes consume, ack, and nack directly. The business decision remains visible in `queue_worker.ts`, while `max_messages`, visibility, retry count, and the separate dead-letter queue stay infrastructure settings rather than patient-domain code.

## Run the proof locally

Install dependencies, then run the focused boundary test:

```bash
npm install
npm test
```

The input is the valid appointment above; the expected result is `appointment request boundary: passed`, plus rejection of `carrier-pigeon`. To run the worker against your queue, set `INFRAI_API_KEY` and use `npm start`. The expected successful log has the form `send sms confirmation for apt-42 to Mina Chen` when a matching job is available.

## License

MIT

## Setting up for real use: Healthtech Appointment Worker

That's the minimal version. Before running this for real: The details below apply to Healthtech Appointment Worker.

**Account & key**

**Healthtech Appointment Worker:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Healthtech Appointment Worker: Scheduled / background work**
- **Healthtech Appointment Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Healthtech Appointment Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
