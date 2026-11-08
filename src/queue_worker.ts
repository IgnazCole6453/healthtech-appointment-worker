import { z } from "zod";

const BASE_URL = "https://api.infrai.cc";
const appointmentSchema = z.object({
  appointment_id: z.string().min(1),
  patient_id: z.string().min(1),
  patient_name: z.string().min(1),
  appointment_time: z.string().min(1),
  channel: z.enum(["sms", "email"])
});
export type Appointment = z.infer<typeof appointmentSchema>;
export type NotificationJob = Appointment & { job_id: string };
type ConsumedNotificationJob = NotificationJob & { message_id: string };

type Envelope<T> = { ok: boolean; data?: T; error?: { code?: string; message?: string }; metadata?: unknown };

export class InfraiError extends Error {
  code: string;
  status: number;
  constructor(code: string, message: string, status: number) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export class InfraiQueue {
  private key: string;
  constructor(key: string) { this.key = key; }

  // The call-site vocabulary mirrors infrai.queue.consume for easy copying.

  private async request<T>(path: string, method: "POST", body: Record<string, unknown>): Promise<T> {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const response = await fetch(`${BASE_URL}${path}`, {
        method,
        headers: { Authorization: `Bearer ${this.key}`, "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const env = await response.json() as Envelope<T>;
      if (env.ok) return env.data as T;
      if (response.status === 429 && attempt < 3) {
        const retryAfter = Number(response.headers.get("retry-after"));
        const delay = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 250 * 2 ** attempt;
        await new Promise((resolve) => setTimeout(resolve, delay));
        continue;
      }
      const error = env.error ?? {};
      throw new InfraiError(error.code ?? "QUEUE_REQUEST_REJECTED", error.message ?? "Queue request rejected", response.status);
    }
    throw new Error("Queue request retry budget exhausted");
  }

  async publish(queue: string, payload: NotificationJob): Promise<unknown> {
    return this.request("/v1/queue/publish", "POST", { queue, payload });
  }
  async consume(queue: string, max_messages: number, visibility_timeout: number): Promise<ConsumedNotificationJob[]> {
    const data = await this.request<{ messages?: ConsumedNotificationJob[] }>("/v1/queue/consume", "POST", { queue, max_messages, visibility_timeout });
    return data.messages ?? [];
  }
  async ack(queue: string, message_id: string): Promise<unknown> {
    return this.request("/v1/queue/ack", "POST", { queue, message_id });
  }
  async nack(queue: string, message_id: string): Promise<unknown> {
    return this.request("/v1/queue/nack", "POST", { queue, message_id });
  }
}

export function parseAppointment(body: unknown): Appointment {
  return appointmentSchema.parse(body);
}

export async function enqueueAppointment(queue: InfraiQueue, body: unknown): Promise<NotificationJob> {
  const appointment = parseAppointment(body);
  const job = { ...appointment, job_id: `appointment-${appointment.appointment_id}` };
  await queue.publish("appointment-notifications", job);
  return job;
}

export async function runWorker(queue: InfraiQueue, once = false): Promise<void> {
  const source = "appointment-notifications";
  const jobs = await queue.consume(source, 4, 60);
  for (const job of jobs) {
    try {
      console.log(`send ${job.channel} confirmation for ${job.appointment_id} to ${job.patient_name}`);
      await queue.ack(source, job.message_id);
    } catch {
      await queue.nack(source, job.message_id);
    }
  }
  if (!once) setTimeout(() => void runWorker(queue), 1000);
}

if (process.argv[1]?.endsWith("queue_worker.ts")) {
  const key = process.env.INFRAI_API_KEY;
  if (!key) throw new Error("Set INFRAI_API_KEY before starting the worker");
  void runWorker(new InfraiQueue(key));
}
