import { parseAppointment } from "./queue_worker.ts";

const valid = parseAppointment({
  appointment_id: "apt-42",
  patient_id: "patient-7",
  patient_name: "Mina Chen",
  appointment_time: "2026-11-04T09:30:00Z",
  channel: "sms"
});
if (valid.appointment_id !== "apt-42") throw new Error("valid appointment was not accepted");
let rejected = false;
try { parseAppointment({ appointment_id: "apt-42", channel: "carrier-pigeon" }); } catch { rejected = true; }
if (!rejected) throw new Error("invalid notification channel was accepted");
console.log("appointment request boundary: passed");
