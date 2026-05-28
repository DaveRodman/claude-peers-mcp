import { test, expect } from "bun:test";
import { shouldReregister } from "./server.ts";

test("re-registers when broker reports the peer is unknown", () => {
  expect(shouldReregister({ ok: true, known: false })).toBe(true);
});

test("does NOT re-register when the broker still knows the peer", () => {
  expect(shouldReregister({ ok: true, known: true })).toBe(false);
});

test("does NOT re-register against an older broker that omits `known`", () => {
  expect(shouldReregister({ ok: true })).toBe(false);
});

test("does NOT re-register on a null/undefined heartbeat result", () => {
  expect(shouldReregister(null)).toBe(false);
  expect(shouldReregister(undefined)).toBe(false);
});
