#!/usr/bin/env bash
# ============================================================
# AIOStreams Monitor Installer for Arrstack
# This script will:
# 1. Clone the latest AIOStreams repo
# 2. Inject the monitor middleware and dashboard files
# 3. Build a custom Docker image (aiostreams-monitor:latest)
# 4. Update your docker-compose.yml to use the new image
# 5. Restart the AIOStreams container
# ============================================================
set -e

echo "=== [1/6] Preparing AIOStreams Custom Build ==="
BUILD_DIR="aiostreams-custom-build"
rm -rf "$BUILD_DIR"
git clone https://github.com/Viren070/AIOStreams.git "$BUILD_DIR"
cd "$BUILD_DIR"

echo "=== [2/6] Writing Monitor Files ==="
mkdir -p packages/server/src/static/monitor

cat << 'EOF' > packages/server/src/middlewares/stream-monitor.ts
/**
 * AIOStreams Stream Monitor Middleware
 *
 * Tracks concurrent stream sessions per user config UUID and actively
 * blocks new streams when the configured concurrent limit is exceeded.
 *
 * Sessions are keyed by the userData segment from the Stremio URL path
 * (e.g. /:userData/stream/movie/:id.json). Each unique userData gets
 * its own UUID-identified session entries.
 *
 * Environment variables:
 *   MONITOR_CONCURRENT_LIMIT  — max concurrent streams per user (default: 4)
 *   MONITOR_SESSION_TIMEOUT   — ms before idle session expires (default: 300000 = 5min)
 */

import express, { Router, Request, Response, NextFunction } from 'express';
import { randomUUID } from 'crypto';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createLogger } from '@aiostreams/core';

const logger = createLogger('stream-monitor');

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface StreamSession {
  uuid: string;
  userConfig: string;
  ip: string;
  userAgent: string;
  contentType: string;
  contentId: string;
  contentTitle: string;
  firstSeen: number;
  lastSeen: number;
  requestPath: string;
}

export interface Violation {
  uuid: string;
  timestamp: number;
  userConfig: string;
  activeCount: number;
  limit: number;
  ip: string;
  contentId: string;
  blocked: boolean;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

let CONCURRENT_LIMIT = parseInt(process.env.MONITOR_CONCURRENT_LIMIT || '4', 10);
let SESSION_TIMEOUT = parseInt(process.env.MONITOR_SESSION_TIMEOUT || '300000', 10);

// ---------------------------------------------------------------------------
// In-memory stores
// ---------------------------------------------------------------------------

// Map<sessionUUID, StreamSession>
const sessions = new Map<string, StreamSession>();

// Index: userConfig -> Set<sessionUUID>  (for fast per-user lookup)
const userSessions = new Map<string, Set<string>>();

const violations: Violation[] = [];

// Cache: contentId -> title (resolved via Cinemeta)
const titleCache = new Map<string, string>();

async function resolveTitle(contentType: string, contentId: string): Promise<string> {
  // contentId may contain season:episode suffix like tt1234567:1:2
  const baseId = contentId.split(':')[0];
  const cacheKey = `${contentType}/${baseId}`;

  if (titleCache.has(cacheKey)) {
    const cachedName = titleCache.get(cacheKey)!;
    if (cachedName === baseId) return contentId;

    const parts = contentId.split(':');
    if (parts.length >= 3) {
      return `${cachedName} S${parts[1].padStart(2, '0')}E${parts[2].padStart(2, '0')}`;
    }
    return cachedName;
  }

  // Mark as pending so we don't fire multiple requests for the same ID
  titleCache.set(cacheKey, baseId);

  try {
    const type = contentType === 'series' ? 'series' : 'movie';
    const url = `https://v3-cinemeta.strem.io/meta/${type}/${baseId}.json`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (res.ok) {
      const json = await res.json() as { meta?: { name?: string } };
      const name = json?.meta?.name;
      if (name) {
        titleCache.set(cacheKey, name);
        // If it's a series with episode info, append it
        const parts = contentId.split(':');
        if (parts.length >= 3) {
          return `${name} S${parts[1].padStart(2, '0')}E${parts[2].padStart(2, '0')}`;
        }
        return name;
      }
    }
  } catch {
    // Network error or timeout — return raw ID
  }

  return contentId;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Extract the config segment and stream info from a Stremio request path.
 * AIOStreams v2 paths look like: /stremio/:uuid/:encrypted/stream/:type/:id.json
 * or public ones: /stremio/stream/:type/:id.json
 */
const STREAM_PATH_REGEX = /^\/(.+)\/streams?\/([^/]+)\/(.+)\.json$/;

function getActiveSessionsForUser(userConfig: string): StreamSession[] {
  const uuids = userSessions.get(userConfig);
  if (!uuids) return [];
  const result: StreamSession[] = [];
  for (const uuid of uuids) {
    const s = sessions.get(uuid);
    if (s) result.push(s);
  }
  return result;
}

function deleteSession(uuid: string): boolean {
  const session = sessions.get(uuid);
  if (!session) return false;
  sessions.delete(uuid);
  const userSet = userSessions.get(session.userConfig);
  if (userSet) {
    userSet.delete(uuid);
    if (userSet.size === 0) userSessions.delete(session.userConfig);
  }
  return true;
}

// ---------------------------------------------------------------------------
// Cleanup: expire stale sessions
// ---------------------------------------------------------------------------

setInterval(() => {
  const now = Date.now();
  for (const [uuid, session] of sessions) {
    if (now - session.lastSeen > SESSION_TIMEOUT) {
      deleteSession(uuid);
    }
  }
}, 30_000);

// ---------------------------------------------------------------------------
// Middleware: track stream requests & enforce limits
// ---------------------------------------------------------------------------

export async function streamMonitorMiddleware(
  req: Request,
  res: Response,
  next: NextFunction
) {
  const match = req.path.match(STREAM_PATH_REGEX);
  
  if (!match) {
    // If the URL contains stream, log why it failed the regex
    if (req.path.includes('/stream')) {
      logger.debug(`[Monitor] Ignored stream-like path: ${req.path}`);
    }
    return next();
  }

  let [, userData, contentType, rawContentId] = match;
  // Fully URL-decode the content ID recursively to handle single/double/triple encoding
  let contentId = rawContentId;
  try {
    let prev = '';
    while (contentId !== prev && contentId.includes('%')) {
      prev = contentId;
      contentId = decodeURIComponent(contentId);
    }
  } catch {
    contentId = decodeURIComponent(rawContentId);
  }
  logger.info(`[Monitor] Detected stream request: User=${userData.substring(0, 15)}... Type=${contentType} ID=${contentId}`);
  
  const ipAddress =
    (req.headers['x-forwarded-for'] as string)?.split(',')[0]?.trim() ||
    req.socket.remoteAddress ||
    'unknown';
  const userAgent = req.headers['user-agent'] || 'Unknown';

  // AIOStreams v2 uses /stremio/stream for public, and /stremio/:uuid/:enc/stream for auth.
  // If userData is just 'stremio' or 'chilllink' (no UUID), we track by IP so they don't share one global limit.
  if (userData === 'stremio' || userData === 'chilllink') {
    userData = `public-${ipAddress}`;
  }

  // --- Find or create session for this user + content ---
  // A "session" is one user streaming one piece of content.
  // Same user + same content = same session (refresh/heartbeat).
  // Same user + different content = new concurrent session.
  const sessionKey = `${userData}::${contentId}`;
  let existingSession: StreamSession | undefined;

  const userUUIDs = userSessions.get(userData);
  if (userUUIDs) {
    for (const uuid of userUUIDs) {
      const s = sessions.get(uuid);
      if (s && s.contentId === contentId) {
        existingSession = s;
        break;
      }
    }
  }

  if (existingSession) {
    // Heartbeat — update lastSeen
    existingSession.lastSeen = Date.now();
    existingSession.ip = ipAddress;
    existingSession.userAgent = userAgent;
    existingSession.requestPath = req.path;
    return next();
  }

  // --- New stream request: check concurrent limit ---
  const currentCount = userUUIDs ? userUUIDs.size : 0;

  if (currentCount >= CONCURRENT_LIMIT) {
    // BLOCK: user already at or above their limit
    const violation: Violation = {
      uuid: randomUUID(),
      timestamp: Date.now(),
      userConfig: userData,
      activeCount: currentCount + 1,
      limit: CONCURRENT_LIMIT,
      ip: ipAddress,
      contentId,
      blocked: true,
    };
    violations.push(violation);
    if (violations.length > 1000) violations.splice(0, violations.length - 1000);

    logger.warn(
      `Blocked stream for user ${userData.substring(0, 8)}… — ` +
        `${currentCount}/${CONCURRENT_LIMIT} concurrent streams already active`
    );

    // Return a Stremio-compatible response with an error stream
    res.json({
      streams: [
        {
          name: 'AIOStreams',
          title: `⛔ Concurrent stream limit reached (${currentCount}/${CONCURRENT_LIMIT})`,
          description:
            'You have reached the maximum number of concurrent streams. ' +
            'Stop another stream before starting a new one.',
          url: '#',
          behaviorHints: { notWebReady: true },
        },
      ],
    });
    return;
  }

  // --- Allow: create new session ---
  const title = await resolveTitle(contentType, contentId);
  const newSession: StreamSession = {
    uuid: randomUUID(),
    userConfig: userData,
    ip: ipAddress,
    userAgent,
    contentType,
    contentId,
    contentTitle: title,
    firstSeen: Date.now(),
    lastSeen: Date.now(),
    requestPath: req.path,
  };

  sessions.set(newSession.uuid, newSession);
  if (!userSessions.has(userData)) {
    userSessions.set(userData, new Set());
  }
  userSessions.get(userData)!.add(newSession.uuid);

  // Warn if approaching limit
  const newCount = (userSessions.get(userData)?.size ?? 0);
  if (newCount >= CONCURRENT_LIMIT) {
    logger.info(
      `User ${userData.substring(0, 8)}… at capacity: ${newCount}/${CONCURRENT_LIMIT} streams`
    );
  }

  next();
}

// ---------------------------------------------------------------------------
// Monitor API Router
// ---------------------------------------------------------------------------

export function createMonitorRouter(): Router {
  const router = Router();

  // --- API Routes ---

  // All sessions
  router.get('/api/sessions', (_req: Request, res: Response) => {
    const allSessions = [...sessions.values()];
    const uniqueUsers = new Set(allSessions.map((s) => s.userConfig)).size;
    const uniqueIPs = new Set(allSessions.map((s) => s.ip)).size;

    res.json({
      sessions: allSessions.map((s) => {
        // Try to get a fresh title from cache if session still has raw ID
        const baseId = s.contentId.split(':')[0];
        const cacheKey = `${s.contentType}/${baseId}`;
        const cachedTitle = titleCache.get(cacheKey);
        let displayTitle = s.contentTitle;
        if (cachedTitle && cachedTitle !== baseId) {
          displayTitle = cachedTitle;
          const parts = s.contentId.split(':');
          if (parts.length >= 3) {
            displayTitle = `${cachedTitle} S${parts[1].padStart(2, '0')}E${parts[2].padStart(2, '0')}`;
          }
        }
        return {
          ...s,
          contentTitle: displayTitle,
          userConfigShort: s.userConfig.substring(0, 12) + '…',
          duration: Date.now() - s.firstSeen,
        };
      }),
      concurrentLimit: CONCURRENT_LIMIT,
      activeCount: allSessions.length,
      uniqueUsers,
      uniqueIPs,
    });
  });

  // Sessions for a specific user config
  router.get('/api/sessions/user/:userConfig', (req: Request, res: Response) => {
    const userConfig = req.params.userConfig as string;
    const userSessionsList = getActiveSessionsForUser(userConfig);
    res.json({
      sessions: userSessionsList,
      count: userSessionsList.length,
      limit: CONCURRENT_LIMIT,
    });
  });

  // Violations
  router.get('/api/violations', (req: Request, res: Response) => {
    const since = Number(req.query.since) || Date.now() - 86400000;
    const filtered = violations.filter((v) => v.timestamp >= since);
    res.json({
      violations: filtered,
      total: violations.length,
      blocked: filtered.filter((v) => v.blocked).length,
    });
  });

  // Stats summary
  router.get('/api/stats', (_req: Request, res: Response) => {
    const now = Date.now();
    const allSessions = [...sessions.values()];
    res.json({
      activeSessions: allSessions.length,
      concurrentLimit: CONCURRENT_LIMIT,
      sessionTimeout: SESSION_TIMEOUT,
      violations24h: violations.filter((v) => now - v.timestamp < 86400000)
        .length,
      blocked24h: violations.filter(
        (v) => now - v.timestamp < 86400000 && v.blocked
      ).length,
      uniqueUsers: new Set(allSessions.map((s) => s.userConfig)).size,
      uniqueIPs: new Set(allSessions.map((s) => s.ip)).size,
      capacityByUser: Object.fromEntries(
        [...userSessions.entries()].map(([user, uuids]) => [
          user.substring(0, 12) + '…',
          { active: uuids.size, limit: CONCURRENT_LIMIT },
        ])
      ),
    });
  });

  // Terminate a session by UUID
  router.post('/api/sessions/:uuid/terminate', (req: Request, res: Response) => {
    const uuid = req.params.uuid as string;
    if (deleteSession(uuid)) {
      logger.info(`Session ${uuid} terminated via monitor API`);
      res.json({ success: true, message: `Session ${uuid} terminated` });
    } else {
      res.status(404).json({ success: false, message: 'Session not found' });
    }
  });

  // Update settings at runtime
  router.put('/api/settings', (req: Request, res: Response) => {
    if (req.body.concurrentLimit != null) {
      CONCURRENT_LIMIT = Math.max(1, parseInt(req.body.concurrentLimit, 10));
    }
    if (req.body.sessionTimeout != null) {
      SESSION_TIMEOUT = Math.max(10000, parseInt(req.body.sessionTimeout, 10));
    }
    logger.info(
      `Monitor settings updated: limit=${CONCURRENT_LIMIT}, timeout=${SESSION_TIMEOUT}ms`
    );
    res.json({ concurrentLimit: CONCURRENT_LIMIT, sessionTimeout: SESSION_TIMEOUT });
  });

  // --- Network Stats (reads /proc/net/dev on Linux) ---
  let prevNetStats: { timestamp: number; rxBytes: number; txBytes: number } | null = null;

  let networkSource = 'none';

  function readNetworkStats(): { rxBytes: number; txBytes: number } | null {
    // Try host's /proc/net/dev first (mounted via volume), fall back to container's own
    const procPaths = ['/host/proc/net/dev', '/proc/net/dev'];
    for (const procPath of procPaths) {
      try {
        const data = fs.readFileSync(procPath, 'utf-8');
        const lines = data.split('\n').slice(2); // skip headers
        let totalRx = 0;
        let totalTx = 0;
        for (const line of lines) {
          const parts = line.trim().split(/\s+/);
          if (parts.length < 10) continue;
          const iface = parts[0].replace(':', '');
          // Exclude virtual/local loopback interfaces
          const isVirtualOrLoopback = /^(lo|docker|br-|veth|wg|tailscale|cni|cali|flannel|tun|tap|member|vnet|virbr)/i.test(iface);
          if (isVirtualOrLoopback) continue;
          totalRx += parseInt(parts[1], 10) || 0;
          totalTx += parseInt(parts[9], 10) || 0;
        }

        // Fallback: if everything was excluded, count any interface except loopback
        if (totalRx === 0 && totalTx === 0) {
          for (const line of lines) {
            const parts = line.trim().split(/\s+/);
            if (parts.length < 10) continue;
            const iface = parts[0].replace(':', '');
            if (/^lo/i.test(iface)) continue;
            totalRx += parseInt(parts[1], 10) || 0;
            totalTx += parseInt(parts[9], 10) || 0;
          }
        }

        if (networkSource !== procPath) {
          networkSource = procPath;
          logger.info(`[Monitor] Reading network stats from: ${procPath}`);
          // Log available interfaces for debugging
          const ifaces = lines
            .map(l => l.trim().split(/\s+/)[0]?.replace(':', ''))
            .filter(Boolean);
          logger.info(`[Monitor] Available interfaces: ${ifaces.join(', ')}`);
        }
        return { rxBytes: totalRx, txBytes: totalTx };
      } catch {
        continue;
      }
    }
    return null;
  }

  router.get('/api/network', (_req: Request, res: Response) => {
    const current = readNetworkStats();
    if (!current) {
      res.status(503).json({ error: 'Network stats not available (non-Linux OS)' });
      return;
    }

    const now = Date.now();
    let rxRate = 0;
    let txRate = 0;

    if (prevNetStats) {
      const elapsed = (now - prevNetStats.timestamp) / 1000; // seconds
      if (elapsed > 0) {
        rxRate = (current.rxBytes - prevNetStats.rxBytes) / elapsed;
        txRate = (current.txBytes - prevNetStats.txBytes) / elapsed;
      }
    }

    prevNetStats = { timestamp: now, rxBytes: current.rxBytes, txBytes: current.txBytes };

    res.json({
      source: networkSource,
      totalRxBytes: current.rxBytes,
      totalTxBytes: current.txBytes,
      rxBytesPerSec: Math.max(0, Math.round(rxRate)),
      txBytesPerSec: Math.max(0, Math.round(txRate)),
    });
  });

  // --- Static dashboard files ---
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const dashboardPath = path.join(__dirname, '../static/monitor');
  router.use('/', (req: Request, res: Response, next: NextFunction) => {
    // Only serve static files for non-API paths
    if (req.path.startsWith('/api/')) return next();
    return express.static(dashboardPath)(req, res, next);
  });

  // Fallback: serve index.html for root
  router.get('/', (_req: Request, res: Response) => {
    res.sendFile(path.join(dashboardPath, 'index.html'));
  });

  return router;
}

EOF

cat << 'EOF' > packages/server/src/static/monitor/app.js
// ============================================
// AIOStreams Monitor — Dashboard Application
// ============================================

(function() {
    'use strict';

    // ============================================
    // State & Configuration
    // ============================================

    // API base path — when served from AIOStreams, this is relative.
    // When opened standalone (file://), API calls will fail and we fall back to demo mode.
    const API_BASE = '/monitor/api';
    let isLiveMode = false;

    const state = {
        concurrentLimit: 4,
        alertThreshold: 75, // percentage
        debridService: 'realdebrid',
        audioAlerts: true,
        autoTerminate: false,
        sessions: [],
        alerts: [],
        violations: [],
        timelineData: [],
        violationHistoryData: [],
        selectedRange: '1h',
    };

    // ============================================
    // API Client
    // ============================================
    async function apiFetch(path, options = {}) {
        const res = await fetch(`${API_BASE}${path}`, {
            headers: { 'Content-Type': 'application/json' },
            ...options,
        });
        if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
        return res.json();
    }

    async function detectLiveMode() {
        try {
            const data = await apiFetch('/stats');
            if (data && typeof data.activeSessions === 'number') {
                isLiveMode = true;
                state.concurrentLimit = data.concurrentLimit || 4;
                return true;
            }
        } catch (e) {
            // API not available — demo mode
        }
        isLiveMode = false;
        return false;
    }

    // Debrid services metadata
    const DEBRID_SERVICES = {
        realdebrid:  { name: 'Real-Debrid',   color: '#6366f1', maxStreams: 4 },
        alldebrid:   { name: 'AllDebrid',      color: '#22d3ee', maxStreams: 3 },
        premiumize:  { name: 'Premiumize',     color: '#a78bfa', maxStreams: 5 },
        debridlink:  { name: 'Debrid-Link',    color: '#34d399', maxStreams: 3 },
        torbox:      { name: 'TorBox',         color: '#fb923c', maxStreams: 6 },
    };

    // Sample data pools
    const USERS = [
        { name: 'Alice M.', device: 'Shield TV', initials: 'AM' },
        { name: 'Bob K.', device: 'Fire Stick 4K', initials: 'BK' },
        { name: 'Charlie D.', device: 'Apple TV', initials: 'CD' },
        { name: 'Diana P.', device: 'iPhone 15', initials: 'DP' },
        { name: 'Ethan R.', device: 'Samsung TV', initials: 'ER' },
        { name: 'Fiona L.', device: 'iPad Pro', initials: 'FL' },
        { name: 'George W.', device: 'MacBook Pro', initials: 'GW' },
        { name: 'Hannah S.', device: 'Chromecast', initials: 'HS' },
    ];

    const CONTENT = [
        'Breaking Bad S05E16', 'The Bear S03E01', 'Dune: Part Two (2024)',
        'Oppenheimer (2023)', 'Severance S02E08', 'The Last of Us S02E03',
        'Shogun S01E10', 'Fallout S01E06', 'House of the Dragon S02E04',
        'Arcane S02E09', 'Interstellar (2014)', 'The Penguin S01E05',
        'Squid Game S02E07', 'Andor S02E12', 'Blade Runner 2049 (2017)',
    ];

    const IPS = [
        '192.168.1.42', '10.0.0.15', '172.16.0.88', '192.168.0.101',
        '10.0.1.33', '192.168.2.77', '203.0.113.5', '198.51.100.12',
    ];

    const SERVICES = ['realdebrid', 'alldebrid', 'premiumize', 'torbox', 'debridlink'];
    const STATUSES = ['streaming', 'streaming', 'streaming', 'buffering', 'paused'];
    const AVATAR_COLORS = [
        'linear-gradient(135deg, #6366f1, #8b5cf6)',
        'linear-gradient(135deg, #22d3ee, #06b6d4)',
        'linear-gradient(135deg, #f43f5e, #e11d48)',
        'linear-gradient(135deg, #fb923c, #f59e0b)',
        'linear-gradient(135deg, #34d399, #10b981)',
        'linear-gradient(135deg, #a78bfa, #7c3aed)',
        'linear-gradient(135deg, #f472b6, #ec4899)',
        'linear-gradient(135deg, #38bdf8, #0284c7)',
    ];

    // ============================================
    // DOM Elements
    // ============================================
    const dom = {};
    function cacheDom() {
        dom.activeStreamCount = document.getElementById('activeStreamCount');
        dom.concurrentLimit = document.getElementById('concurrentLimit');
        dom.violationCount = document.getElementById('violationCount');
        dom.uniqueIPCount = document.getElementById('uniqueIPCount');
        dom.streamTrend = document.getElementById('streamTrend');
        dom.streamTrendText = document.getElementById('streamTrendText');
        dom.violationTrend = document.getElementById('violationTrend');
        dom.violationTrendText = document.getElementById('violationTrendText');
        dom.capacityFill = document.getElementById('capacityFill');
        dom.ipWarning = document.getElementById('ipWarning');
        dom.limitLineValue = document.getElementById('limitLineValue');
        dom.limitLine = document.getElementById('limitLine');
        dom.sessionsBody = document.getElementById('sessionsBody');
        dom.alertsList = document.getElementById('alertsList');
        dom.alertBadge = document.getElementById('alertBadge');
        dom.donutLegend = document.getElementById('donutLegend');
        dom.donutTotal = document.getElementById('donutTotal');
        dom.timelineCanvas = document.getElementById('timelineCanvas');
        dom.donutCanvas = document.getElementById('donutCanvas');
        dom.violationsCanvas = document.getElementById('violationsCanvas');
        dom.sessionSearch = document.getElementById('sessionSearch');
        dom.lastUpdated = document.getElementById('lastUpdated');
        dom.toastContainer = document.getElementById('toastContainer');
        dom.settingsModal = document.getElementById('settingsModal');
        dom.settingsBtn = document.getElementById('settingsBtn');
        dom.settingsClose = document.getElementById('settingsClose');
        dom.settingsCancel = document.getElementById('settingsCancel');
        dom.settingsSave = document.getElementById('settingsSave');
        dom.limitSlider = document.getElementById('limitSlider');
        dom.limitDisplay = document.getElementById('limitDisplay');
        dom.thresholdSlider = document.getElementById('thresholdSlider');
        dom.thresholdDisplay = document.getElementById('thresholdDisplay');
        dom.debridSelect = document.getElementById('debridSelect');
        dom.audioToggle = document.getElementById('audioToggle');
        dom.autoTermToggle = document.getElementById('autoTermToggle');
        dom.clearAlertsBtn = document.getElementById('clearAlertsBtn');
        dom.kpiViolations = document.getElementById('kpiViolations');
        dom.connectionStatus = document.getElementById('connectionStatus');
        dom.alertsBtn = document.getElementById('alertsBtn');
        dom.networkBandwidth = document.getElementById('networkBandwidth');
        dom.networkDetails = document.getElementById('networkDetails');
    }

    // ============================================
    // Utility Functions
    // ============================================
    function rand(min, max) {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    function pick(arr) {
        return arr[rand(0, arr.length - 1)];
    }

    function deterministicPick(str, arr) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            hash = str.charCodeAt(i) + ((hash << 5) - hash);
        }
        return arr[Math.abs(hash) % arr.length];
    }

    function formatDuration(ms) {
        const secs = Math.floor(ms / 1000);
        const mins = Math.floor(secs / 60);
        const hrs = Math.floor(mins / 60);
        if (hrs > 0) return `${hrs}h ${mins % 60}m`;
        if (mins > 0) return `${mins}m ${secs % 60}s`;
        return `${secs}s`;
    }

    function timeAgo(ts) {
        const diff = Date.now() - ts;
        const secs = Math.floor(diff / 1000);
        if (secs < 60) return 'just now';
        const mins = Math.floor(secs / 60);
        if (mins < 60) return `${mins}m ago`;
        const hrs = Math.floor(mins / 60);
        return `${hrs}h ago`;
    }

    function generateUUID() {
        // RFC 4122 version 4 UUID
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = (Math.random() * 16) | 0;
            const v = c === 'x' ? r : (r & 0x3) | 0x8;
            return v.toString(16);
        });
    }

    function truncateUUID(uuid) {
        return uuid.substring(0, 8) + '…';
    }

    // ============================================
    // Session Management
    // ============================================
    function createSession() {
        const user = pick(USERS);
        const uuid = generateUUID();
        return {
            id: uuid,
            user: user.name,
            device: user.device,
            initials: user.initials,
            avatarColor: pick(AVATAR_COLORS),
            service: pick(SERVICES),
            ip: pick(IPS),
            content: pick(CONTENT),
            status: 'streaming',
            startTime: Date.now() - rand(10000, 3600000),
        };
    }

    function initSessionsDemo() {
        const count = rand(2, state.concurrentLimit);
        state.sessions = [];
        for (let i = 0; i < count; i++) {
            state.sessions.push(createSession());
        }
    }

    async function initSessionsLive() {
        try {
            const data = await apiFetch('/sessions');
            state.concurrentLimit = data.concurrentLimit || state.concurrentLimit;
            // Map API sessions to dashboard format
            state.sessions = data.sessions.map(s => {
                let actualUserId = s.userConfig;
                const uuidMatch = actualUserId.match(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
                if (uuidMatch) {
                    actualUserId = uuidMatch[1];
                } else if (actualUserId.startsWith('public-')) {
                    actualUserId = 'IP: ' + actualUserId.substring(7);
                } else if (actualUserId.startsWith('stremio/')) {
                    actualUserId = actualUserId.split('/')[1] || actualUserId;
                }

                return {
                    id: actualUserId, // Put the real user UUID here instead of the random session UUID
                    user: actualUserId.substring(0, 12) + '…',
                    device: s.userAgent?.substring(0, 45) || 'Unknown Device',
                    initials: (actualUserId || 'U').substring(0, 2).toUpperCase(),
                    avatarColor: deterministicPick(actualUserId, AVATAR_COLORS),
                    service: detectServiceFromAgent(s.userAgent),
                    ip: s.ip,
                    content: s.contentTitle || (s.contentType + ' / ' + s.contentId),
                    status: 'streaming',
                    startTime: s.firstSeen,
                    sessionUuid: s.uuid // keep internal reference
                };
            });
        } catch (e) {
            console.warn('Failed to fetch sessions from API:', e);
        }
    }

    function detectServiceFromAgent(ua) {
        if (!ua) return state.debridService || 'torbox';
        const lower = ua.toLowerCase();
        if (lower.includes('stremio')) return state.debridService || 'torbox';
        if (lower.includes('torbox')) return 'torbox';
        if (lower.includes('realdebrid') || lower.includes('real-debrid')) return 'realdebrid';
        return state.debridService || 'torbox'; // Use the user's default setting (TorBox) instead of random
    }

    function addSession() {
        const session = createSession();
        state.sessions.push(session);

        // Check for violation
        if (state.sessions.length > state.concurrentLimit) {
            session.status = 'violation';
            triggerViolation(session);
        } else {
            const pct = (state.sessions.length / state.concurrentLimit) * 100;
            if (pct >= state.alertThreshold) {
                addAlert('warning', 'Approaching Limit',
                    `${state.sessions.length}/${state.concurrentLimit} concurrent streams active (${Math.round(pct)}% capacity)`,
                    Date.now());
            }
        }

        updateAll();
    }

    async function removeSession(id) {
        if (isLiveMode) {
            try {
                await apiFetch(`/sessions/${id}/terminate`, { method: 'POST' });
            } catch (e) {
                showToast('critical', 'Terminate Failed', `Could not terminate session: ${e.message}`);
                return;
            }
        }
        state.sessions = state.sessions.filter(s => s.id !== id);
        updateAll();
        showToast('info', 'Session Terminated', `Session ${truncateUUID(id)} has been terminated`);
    }

    function triggerViolation(session) {
        state.violations.push({
            timestamp: Date.now(),
            sessionId: session.id,
            user: session.user,
            ip: session.ip,
            count: state.sessions.length,
            limit: state.concurrentLimit,
        });

        addAlert('critical', 'Concurrent Stream Violation!',
            `${state.sessions.length} active streams exceed limit of ${state.concurrentLimit}. Session: ${truncateUUID(session.id)} — ${session.user} (${session.ip})`,
            Date.now());

        showToast('critical', '🚨 Stream Violation Detected',
            `${state.sessions.length}/${state.concurrentLimit} concurrent streams — limit exceeded!`);

        // Update violation KPI visual
        dom.kpiViolations.classList.add('violation-active');
        setTimeout(() => dom.kpiViolations.classList.remove('violation-active'), 5000);

        if (state.autoTerminate) {
            setTimeout(() => {
                removeSession(session.id);
                showToast('success', 'Auto-terminated', `Session for ${session.user} was automatically terminated`);
            }, 2000);
        }
    }

    // ============================================
    // Alert Management
    // ============================================
    function addAlert(type, title, desc, timestamp) {
        state.alerts.unshift({ type, title, desc, timestamp, id: Math.random().toString(36).substr(2, 8) });
        if (state.alerts.length > 50) state.alerts.pop();
        renderAlerts();
        updateAlertBadge();
    }

    function clearAlerts() {
        state.alerts = [];
        renderAlerts();
        updateAlertBadge();
    }

    function updateAlertBadge() {
        const criticalCount = state.alerts.filter(a => a.type === 'critical').length;
        dom.alertBadge.textContent = criticalCount;
        dom.alertBadge.style.display = criticalCount > 0 ? 'flex' : 'none';
    }

    function renderAlerts() {
        if (state.alerts.length === 0) {
            dom.alertsList.innerHTML = `
                <div class="empty-state">
                    <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                        <polyline points="22 4 12 14.01 9 11.01"/>
                    </svg>
                    <span>No alerts — all clear!</span>
                </div>`;
            return;
        }

        dom.alertsList.innerHTML = state.alerts.slice(0, 20).map(alert => {
            const icons = {
                critical: '🔴',
                warning: '🟡',
                info: '🔵',
                success: '🟢',
            };
            return `
                <div class="alert-item">
                    <div class="alert-icon ${alert.type}">${icons[alert.type]}</div>
                    <div class="alert-body">
                        <div class="alert-title">${alert.title}</div>
                        <div class="alert-desc">${alert.desc}</div>
                        <div class="alert-time">${timeAgo(alert.timestamp)}</div>
                    </div>
                </div>`;
        }).join('');
    }

    // ============================================
    // Toast Notifications
    // ============================================
    function showToast(type, title, message) {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <div class="toast-title">${title}</div>
                <div class="toast-message">${message}</div>
            </div>
            <button class="toast-close" onclick="this.closest('.toast').remove()">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
            <div class="toast-progress"></div>`;

        dom.toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.classList.add('removing');
            setTimeout(() => toast.remove(), 300);
        }, 4000);
    }

    // ============================================
    // Rendering
    // ============================================
    function updateKPIs() {
        const activeCount = state.sessions.length;
        const prevCount = parseInt(dom.activeStreamCount.textContent) || 0;

        // Animate count
        animateValue(dom.activeStreamCount, prevCount, activeCount, 400);

        // Concurrent limit
        dom.concurrentLimit.textContent = state.concurrentLimit;
        dom.limitLineValue.textContent = state.concurrentLimit;

        // Capacity bar
        const pct = Math.min((activeCount / state.concurrentLimit) * 100, 100);
        dom.capacityFill.style.width = pct + '%';
        dom.capacityFill.className = 'capacity-fill';
        if (pct >= 100) dom.capacityFill.classList.add('danger');
        else if (pct >= 75) dom.capacityFill.classList.add('warning');

        // Violations (24h)
        const violations24h = state.violations.filter(v => Date.now() - v.timestamp < 86400000).length;
        dom.violationCount.textContent = violations24h;

        // Unique IPs
        const uniqueIPs = new Set(state.sessions.map(s => s.ip)).size;
        dom.uniqueIPCount.textContent = uniqueIPs;

        // IP warning
        const ipEl = dom.ipWarning;
        if (uniqueIPs > 3) {
            ipEl.innerHTML = '<span class="ip-danger">⚠ Multiple IPs detected</span>';
        } else if (uniqueIPs > 2) {
            ipEl.innerHTML = '<span class="ip-warning">Moderate IP diversity</span>';
        } else {
            ipEl.innerHTML = '<span class="ip-safe">Within safe range</span>';
        }

        // Trends
        const diff = activeCount - prevCount;
        if (diff >= 0) {
            dom.streamTrend.className = 'kpi-trend up';
            dom.streamTrendText.textContent = `+${diff}`;
        } else {
            dom.streamTrend.className = 'kpi-trend down';
            dom.streamTrendText.textContent = `${diff}`;
        }
    }

    function formatBytesRate(bytes) {
        if (bytes < 1024) return bytes + ' B/s';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB/s';
        if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB/s';
        return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB/s';
    }

    function formatBytesTotal(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
        if (bytes < 1024 * 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
        return (bytes / (1024 * 1024 * 1024 * 1024)).toFixed(2) + ' TB';
    }

    async function updateNetworkStats() {
        if (!isLiveMode) {
            // Demo mode: show simulated values
            const fakeRx = rand(500000, 8000000);
            const fakeTx = rand(200000, 3000000);
            if (dom.networkBandwidth) {
                dom.networkBandwidth.textContent = formatBytesRate(fakeRx + fakeTx);
            }
            if (dom.networkDetails) {
                dom.networkDetails.innerHTML = `<span class="network-stats">↓ ${formatBytesRate(fakeRx)} &nbsp; ↑ ${formatBytesRate(fakeTx)}</span>`;
            }
            return;
        }
        try {
            const data = await apiFetch('/network');
            const totalRate = data.rxBytesPerSec + data.txBytesPerSec;
            if (dom.networkBandwidth) {
                dom.networkBandwidth.textContent = formatBytesRate(totalRate);
            }
            if (dom.networkDetails) {
                let warning = '';
                if (data.source === '/proc/net/dev') {
                    warning = ` <span style="color: #f43f5e; cursor: help; font-weight: 600;" title="Container only - Host /proc/net/dev mount missing!">(Container Only)</span>`;
                }
                dom.networkDetails.innerHTML = `<span class="network-stats">↓ ${formatBytesRate(data.rxBytesPerSec)} &nbsp; ↑ ${formatBytesRate(data.txBytesPerSec)}${warning}</span>`;
            }
        } catch (e) {
            if (dom.networkBandwidth) {
                dom.networkBandwidth.textContent = 'N/A';
            }
            if (dom.networkDetails) {
                dom.networkDetails.innerHTML = `<span class="network-stats">Unavailable</span>`;
            }
        }
    }

    function animateValue(el, start, end, duration) {
        const startTime = performance.now();
        const diff = end - start;

        function step(time) {
            const progress = Math.min((time - startTime) / duration, 1);
            const ease = 1 - Math.pow(1 - progress, 3); // ease-out cubic
            el.textContent = Math.round(start + diff * ease);
            if (progress < 1) requestAnimationFrame(step);
        }

        requestAnimationFrame(step);
    }

    function renderSessions() {
        const filter = (dom.sessionSearch.value || '').toLowerCase();
        const filtered = state.sessions.filter(s => {
            if (!filter) return true;
            return s.id.toLowerCase().includes(filter)
                || s.user.toLowerCase().includes(filter)
                || s.device.toLowerCase().includes(filter)
                || s.ip.includes(filter)
                || s.content.toLowerCase().includes(filter)
                || s.status.includes(filter);
        });

        if (filtered.length === 0) {
            dom.sessionsBody.innerHTML = `
                <tr>
                    <td colspan="8">
                        <div class="empty-state">
                            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                                <circle cx="12" cy="12" r="10"/>
                                <line x1="12" y1="8" x2="12" y2="12"/>
                                <line x1="12" y1="16" x2="12.01" y2="16"/>
                            </svg>
                            <span>No active sessions</span>
                        </div>
                    </td>
                </tr>`;
            return;
        }

        dom.sessionsBody.innerHTML = filtered.map(s => {
            const dur = formatDuration(Date.now() - s.startTime);
            const service = DEBRID_SERVICES[s.service] || DEBRID_SERVICES.realdebrid;
            const statusClass = s.status;
            const statusLabel = s.status.charAt(0).toUpperCase() + s.status.slice(1);

            return `
                <tr data-id="${s.id}">
                    <td>
                        <div class="session-uuid-cell">
                            <span class="session-uuid" title="${s.id}">${truncateUUID(s.id)}</span>
                            <button class="btn-copy-uuid" onclick="window.__copyUUID('${s.id}')" title="Copy full UUID">
                                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
                            </button>
                        </div>
                    </td>
                    <td>
                        <div class="session-user">
                            <div class="session-avatar" style="background: ${s.avatarColor}">${s.initials}</div>
                            <div class="session-user-info">
                                <span class="session-username">${s.user}</span>
                                <span class="session-device">${s.device}</span>
                            </div>
                        </div>
                    </td>
                    <td>
                        <span class="status-badge" style="background: ${hexToRgba(service.color, 0.1)}; color: ${service.color}">
                            <span class="status-badge-dot" style="background: ${service.color}"></span>
                            ${service.name}
                        </span>
                    </td>
                    <td><span class="session-ip">${s.ip}</span></td>
                    <td><span class="session-content" title="${s.content}">${s.content}</span></td>
                    <td><span class="session-duration">${dur}</span></td>
                    <td>
                        <span class="status-badge ${statusClass}">
                            <span class="status-badge-dot"></span>
                            ${statusLabel}
                        </span>
                    </td>
                    <td>
                        <button class="btn-terminate" onclick="window.__terminateSession('${s.sessionUuid}')">End</button>
                    </td>
                </tr>`;
        }).join('');
    }

    function hexToRgba(hex, alpha) {
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    // ============================================
    // Charts — Timeline (Canvas)
    // ============================================
    function generateTimelineData() {
        const points = 60;
        const data = [];
        const now = Date.now();
        for (let i = 0; i < points; i++) {
            const t = now - (points - i) * 60000;
            // Simulate stream counts with realistic patterns
            let base = rand(1, state.concurrentLimit);
            // Occasionally spike above limit
            if (Math.random() < 0.12) base = state.concurrentLimit + rand(1, 2);
            data.push({ time: t, count: Math.max(0, base) });
        }
        state.timelineData = data;
    }

    function drawTimeline() {
        const canvas = dom.timelineCanvas;
        const ctx = canvas.getContext('2d');
        const container = canvas.parentElement;
        const dpr = window.devicePixelRatio || 1;

        canvas.width = container.clientWidth * dpr;
        canvas.height = (container.clientHeight - 30) * dpr;
        ctx.scale(dpr, dpr);

        const w = container.clientWidth;
        const h = container.clientHeight - 30;

        ctx.clearRect(0, 0, w, h);

        const data = state.timelineData;
        if (data.length < 2) return;

        const maxVal = Math.max(state.concurrentLimit + 2, ...data.map(d => d.count));
        const padL = 35;
        const padR = 10;
        const padT = 10;
        const padB = 25;
        const chartW = w - padL - padR;
        const chartH = h - padT - padB;

        // Grid lines
        ctx.strokeStyle = 'rgba(255,255,255,0.04)';
        ctx.lineWidth = 1;
        for (let i = 0; i <= maxVal; i++) {
            const y = padT + chartH - (i / maxVal) * chartH;
            ctx.beginPath();
            ctx.moveTo(padL, y);
            ctx.lineTo(w - padR, y);
            ctx.stroke();

            // Labels
            ctx.fillStyle = 'rgba(255,255,255,0.2)';
            ctx.font = '10px "JetBrains Mono", monospace';
            ctx.textAlign = 'right';
            ctx.fillText(i.toString(), padL - 6, y + 3);
        }

        // Time labels
        ctx.fillStyle = 'rgba(255,255,255,0.15)';
        ctx.textAlign = 'center';
        ctx.font = '9px "JetBrains Mono", monospace';
        const labelCount = 6;
        for (let i = 0; i < labelCount; i++) {
            const idx = Math.floor((i / (labelCount - 1)) * (data.length - 1));
            const x = padL + (idx / (data.length - 1)) * chartW;
            const d = new Date(data[idx].time);
            ctx.fillText(`${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`, x, h - 2);
        }

        // Limit line position for HTML overlay
        const limitY = padT + chartH - (state.concurrentLimit / maxVal) * chartH;
        dom.limitLine.style.top = `${limitY + 16}px`;

        // Area fill
        const gradient = ctx.createLinearGradient(0, padT, 0, padT + chartH);
        gradient.addColorStop(0, 'rgba(99, 102, 241, 0.25)');
        gradient.addColorStop(1, 'rgba(99, 102, 241, 0)');

        ctx.beginPath();
        ctx.moveTo(padL, padT + chartH);
        data.forEach((d, i) => {
            const x = padL + (i / (data.length - 1)) * chartW;
            const y = padT + chartH - (d.count / maxVal) * chartH;
            if (i === 0) ctx.lineTo(x, y);
            else {
                // Smooth curve
                const prevX = padL + ((i - 1) / (data.length - 1)) * chartW;
                const prevY = padT + chartH - (data[i - 1].count / maxVal) * chartH;
                const cpx = (prevX + x) / 2;
                ctx.bezierCurveTo(cpx, prevY, cpx, y, x, y);
            }
        });
        ctx.lineTo(padL + chartW, padT + chartH);
        ctx.closePath();
        ctx.fillStyle = gradient;
        ctx.fill();

        // Line
        ctx.beginPath();
        data.forEach((d, i) => {
            const x = padL + (i / (data.length - 1)) * chartW;
            const y = padT + chartH - (d.count / maxVal) * chartH;
            if (i === 0) ctx.moveTo(x, y);
            else {
                const prevX = padL + ((i - 1) / (data.length - 1)) * chartW;
                const prevY = padT + chartH - (data[i - 1].count / maxVal) * chartH;
                const cpx = (prevX + x) / 2;
                ctx.bezierCurveTo(cpx, prevY, cpx, y, x, y);
            }
        });
        ctx.strokeStyle = '#6366f1';
        ctx.lineWidth = 2.5;
        ctx.stroke();

        // Violation markers
        data.forEach((d, i) => {
            if (d.count > state.concurrentLimit) {
                const x = padL + (i / (data.length - 1)) * chartW;
                const y = padT + chartH - (d.count / maxVal) * chartH;

                // Glow
                ctx.beginPath();
                ctx.arc(x, y, 8, 0, Math.PI * 2);
                ctx.fillStyle = 'rgba(244, 63, 94, 0.15)';
                ctx.fill();

                // Point
                ctx.beginPath();
                ctx.arc(x, y, 4, 0, Math.PI * 2);
                ctx.fillStyle = '#f43f5e';
                ctx.fill();
                ctx.strokeStyle = 'rgba(244, 63, 94, 0.4)';
                ctx.lineWidth = 2;
                ctx.stroke();
            }
        });

        // Current point glow
        const lastD = data[data.length - 1];
        const lx = padL + chartW;
        const ly = padT + chartH - (lastD.count / maxVal) * chartH;
        ctx.beginPath();
        ctx.arc(lx, ly, 6, 0, Math.PI * 2);
        ctx.fillStyle = 'rgba(99, 102, 241, 0.2)';
        ctx.fill();
        ctx.beginPath();
        ctx.arc(lx, ly, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = '#6366f1';
        ctx.fill();
    }

    // ============================================
    // Charts — Donut (Canvas)
    // ============================================
    function drawDonut() {
        const canvas = dom.donutCanvas;
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;

        canvas.width = 200 * dpr;
        canvas.height = 200 * dpr;
        canvas.style.width = '200px';
        canvas.style.height = '200px';
        ctx.scale(dpr, dpr);

        const cx = 100, cy = 100, radius = 72, lineWidth = 22;
        ctx.clearRect(0, 0, 200, 200);

        // Count by service
        const counts = {};
        state.sessions.forEach(s => {
            counts[s.service] = (counts[s.service] || 0) + 1;
        });

        const total = state.sessions.length;
        dom.donutTotal.textContent = total;

        if (total === 0) {
            ctx.beginPath();
            ctx.arc(cx, cy, radius, 0, Math.PI * 2);
            ctx.strokeStyle = 'rgba(255,255,255,0.06)';
            ctx.lineWidth = lineWidth;
            ctx.stroke();
            dom.donutLegend.innerHTML = '';
            return;
        }

        // Draw segments
        let startAngle = -Math.PI / 2;
        const entries = Object.entries(counts);
        const gap = 0.04; // gap between segments

        entries.forEach(([service, count]) => {
            const sweepAngle = (count / total) * (Math.PI * 2 - gap * entries.length);
            const serviceData = DEBRID_SERVICES[service] || DEBRID_SERVICES.realdebrid;

            ctx.beginPath();
            ctx.arc(cx, cy, radius, startAngle, startAngle + sweepAngle);
            ctx.strokeStyle = serviceData.color;
            ctx.lineWidth = lineWidth;
            ctx.lineCap = 'round';
            ctx.stroke();

            startAngle += sweepAngle + gap;
        });

        // Legend
        dom.donutLegend.innerHTML = entries.map(([service, count]) => {
            const sd = DEBRID_SERVICES[service] || DEBRID_SERVICES.realdebrid;
            return `<div class="legend-item">
                <span class="legend-dot" style="background: ${sd.color}"></span>
                ${sd.name} (${count})
            </div>`;
        }).join('');
    }

    // ============================================
    // Charts — Violation History Bar Chart
    // ============================================
    function generateViolationHistory() {
        const data = [];
        const now = new Date();
        for (let i = 6; i >= 0; i--) {
            const d = new Date(now);
            d.setDate(d.getDate() - i);
            data.push({
                label: d.toLocaleDateString('en-US', { weekday: 'short' }),
                violations: rand(0, 5),
                warnings: rand(1, 8),
            });
        }
        state.violationHistoryData = data;
    }

    function drawViolationHistory() {
        const canvas = dom.violationsCanvas;
        const ctx = canvas.getContext('2d');
        const container = canvas.parentElement;
        const dpr = window.devicePixelRatio || 1;

        canvas.width = container.clientWidth * dpr;
        canvas.height = container.clientHeight * dpr;
        ctx.scale(dpr, dpr);

        const w = container.clientWidth;
        const h = container.clientHeight;
        ctx.clearRect(0, 0, w, h);

        const data = state.violationHistoryData;
        if (!data.length) return;

        const padL = 35, padR = 10, padT = 10, padB = 25;
        const chartW = w - padL - padR;
        const chartH = h - padT - padB;
        const barGroupWidth = chartW / data.length;
        const barWidth = Math.min(barGroupWidth * 0.3, 18);
        const maxVal = Math.max(10, ...data.map(d => Math.max(d.violations, d.warnings)));

        // Grid
        ctx.strokeStyle = 'rgba(255,255,255,0.04)';
        ctx.lineWidth = 1;
        for (let i = 0; i <= 5; i++) {
            const y = padT + chartH - (i / 5) * chartH;
            ctx.beginPath();
            ctx.moveTo(padL, y);
            ctx.lineTo(w - padR, y);
            ctx.stroke();

            ctx.fillStyle = 'rgba(255,255,255,0.2)';
            ctx.font = '9px "JetBrains Mono", monospace';
            ctx.textAlign = 'right';
            ctx.fillText(Math.round(i / 5 * maxVal).toString(), padL - 6, y + 3);
        }

        data.forEach((d, i) => {
            const groupX = padL + i * barGroupWidth + barGroupWidth / 2;

            // Warning bar
            const wH = (d.warnings / maxVal) * chartH;
            const wX = groupX - barWidth - 2;
            const wY = padT + chartH - wH;

            ctx.beginPath();
            ctx.roundRect(wX, wY, barWidth, wH, [4, 4, 0, 0]);
            ctx.fillStyle = 'rgba(251, 191, 36, 0.3)';
            ctx.fill();

            // Violation bar
            const vH = (d.violations / maxVal) * chartH;
            const vX = groupX + 2;
            const vY = padT + chartH - vH;

            ctx.beginPath();
            ctx.roundRect(vX, vY, barWidth, vH, [4, 4, 0, 0]);
            ctx.fillStyle = 'rgba(244, 63, 94, 0.5)';
            ctx.fill();

            // Label
            ctx.fillStyle = 'rgba(255,255,255,0.2)';
            ctx.font = '9px "JetBrains Mono", monospace';
            ctx.textAlign = 'center';
            ctx.fillText(d.label, groupX, h - 5);
        });

        // Mini legend
        const legY = padT + 2;
        ctx.font = '9px "Inter", sans-serif';

        ctx.fillStyle = 'rgba(251, 191, 36, 0.5)';
        ctx.beginPath();
        ctx.roundRect(w - padR - 135, legY, 8, 8, 2);
        ctx.fill();
        ctx.fillStyle = 'rgba(255,255,255,0.3)';
        ctx.fillText('Warnings', w - padR - 122, legY + 8);

        ctx.fillStyle = 'rgba(244, 63, 94, 0.6)';
        ctx.beginPath();
        ctx.roundRect(w - padR - 62, legY, 8, 8, 2);
        ctx.fill();
        ctx.fillStyle = 'rgba(255,255,255,0.3)';
        ctx.fillText('Violations', w - padR - 49, legY + 8);
    }

    // ============================================
    // Update All
    // ============================================
    function updateAll() {
        updateKPIs();
        renderSessions();
        drawDonut();
        drawTimeline();
        updateAlertBadge();
        dom.lastUpdated.textContent = 'Updated just now';
    }

    // ============================================
    // Simulation Engine
    // ============================================
    let simInterval;

    // ============================================
    // Live Polling (API mode)
    // ============================================
    function startPolling() {
        // Poll sessions every 3 seconds
        setInterval(async () => {
            try {
                await initSessionsLive();

                // Update timeline
                state.timelineData.push({ time: Date.now(), count: state.sessions.length });
                if (state.timelineData.length > 120) state.timelineData.shift();

                // Check violations
                const prevViolationCount = state.violations.length;
                const violationData = await apiFetch('/violations?since=' + (Date.now() - 86400000));
                if (violationData.violations) {
                    state.violations = violationData.violations.map(v => ({
                        timestamp: v.timestamp,
                        sessionId: v.uuid,
                        user: v.userConfig?.substring(0, 12) + '…',
                        ip: v.ip,
                        count: v.activeCount,
                        limit: v.limit,
                    }));

                    // Alert on new violations
                    if (state.violations.length > prevViolationCount) {
                        const latest = violationData.violations[violationData.violations.length - 1];
                        if (latest && latest.blocked) {
                            addAlert('critical', '🚫 Stream Blocked!',
                                `User ${latest.userConfig?.substring(0, 8)}… blocked — ${latest.activeCount}/${latest.limit} streams`,
                                latest.timestamp);
                            showToast('critical', '🚫 Stream Blocked',
                                `Concurrent limit exceeded — stream request was rejected`);
                            dom.kpiViolations.classList.add('violation-active');
                            setTimeout(() => dom.kpiViolations.classList.remove('violation-active'), 5000);
                        }
                    }
                }

                updateAll();
            } catch (e) {
                // Silently handle polling errors
            }
        }, 3000);

        // Update violation history every 30 seconds
        setInterval(() => {
            drawViolationHistory();
        }, 30000);

        // Poll network stats every 3 seconds
        updateNetworkStats(); // initial call
        setInterval(() => {
            updateNetworkStats();
        }, 3000);
    }

    // ============================================
    // Demo Simulation (standalone/offline mode)
    // ============================================
    function startSimulation() {
        // Update timeline data continuously
        setInterval(() => {
            const current = state.sessions.length;
            state.timelineData.push({ time: Date.now(), count: current });
            if (state.timelineData.length > 120) state.timelineData.shift();
            drawTimeline();
        }, 3000);

        // Session churn simulation
        simInterval = setInterval(() => {
            const action = Math.random();

            if (action < 0.35 && state.sessions.length < state.concurrentLimit + 3) {
                addSession();
            } else if (action < 0.55 && state.sessions.length > 1) {
                const idx = rand(0, state.sessions.length - 1);
                state.sessions.splice(idx, 1);
                updateAll();
            } else if (action < 0.7) {
                if (state.sessions.length > 0) {
                    const s = pick(state.sessions);
                    if (s.status !== 'violation') {
                        s.status = pick(STATUSES);
                    }
                    renderSessions();
                }
            } else if (action < 0.82 && state.sessions.length < state.concurrentLimit + 2) {
                addSession();
            }
            renderSessions();
        }, 5000);

        // Periodic alert generation
        setInterval(() => {
            if (Math.random() < 0.15) {
                const messages = [
                    { type: 'info', title: 'IP Change Detected', desc: `Device switched to IP ${pick(IPS)}` },
                    { type: 'warning', title: 'High Latency Warning', desc: `Proxy latency spike: ${rand(200, 800)}ms on ${pick(Object.values(DEBRID_SERVICES)).name}` },
                    { type: 'info', title: 'Session Renewed', desc: `Token refreshed for ${pick(USERS).name}` },
                    { type: 'warning', title: 'Rate Limit Warning', desc: `API rate approaching limit on ${pick(Object.values(DEBRID_SERVICES)).name}` },
                    { type: 'info', title: 'New Device Connected', desc: `${pick(USERS).device} joined the network` },
                ];
                const msg = pick(messages);
                addAlert(msg.type, msg.title, msg.desc, Date.now());
            }
        }, 8000);

        setInterval(() => { drawViolationHistory(); }, 30000);

        // Network stats in demo mode
        updateNetworkStats();
        setInterval(() => { updateNetworkStats(); }, 3000);
    }

    // ============================================
    // Event Handlers
    // ============================================
    function bindEvents() {
        // Terminate session (exposed globally for onclick)
        window.__terminateSession = function(id) {
            removeSession(id);
        };

        // Copy UUID to clipboard
        window.__copyUUID = function(uuid) {
            navigator.clipboard.writeText(uuid).then(() => {
                showToast('success', 'UUID Copied', uuid);
            }).catch(() => {
                // Fallback for non-HTTPS contexts
                const ta = document.createElement('textarea');
                ta.value = uuid;
                ta.style.position = 'fixed';
                ta.style.opacity = '0';
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                document.body.removeChild(ta);
                showToast('success', 'UUID Copied', uuid);
            });
        };

        // Search
        dom.sessionSearch.addEventListener('input', () => {
            renderSessions();
        });

        // Settings modal
        dom.settingsBtn.addEventListener('click', () => {
            dom.limitSlider.value = state.concurrentLimit;
            dom.limitDisplay.textContent = state.concurrentLimit;
            dom.thresholdSlider.value = state.alertThreshold;
            dom.thresholdDisplay.textContent = state.alertThreshold + '%';
            dom.debridSelect.value = state.debridService;
            dom.audioToggle.checked = state.audioAlerts;
            dom.autoTermToggle.checked = state.autoTerminate;
            dom.settingsModal.classList.add('active');
        });

        function closeSettings() {
            dom.settingsModal.classList.remove('active');
        }

        dom.settingsClose.addEventListener('click', closeSettings);
        dom.settingsCancel.addEventListener('click', closeSettings);

        dom.settingsModal.addEventListener('click', (e) => {
            if (e.target === dom.settingsModal) closeSettings();
        });

        dom.limitSlider.addEventListener('input', () => {
            dom.limitDisplay.textContent = dom.limitSlider.value;
        });

        dom.thresholdSlider.addEventListener('input', () => {
            dom.thresholdDisplay.textContent = dom.thresholdSlider.value + '%';
        });

        dom.settingsSave.addEventListener('click', async () => {
            state.concurrentLimit = parseInt(dom.limitSlider.value);
            state.alertThreshold = parseInt(dom.thresholdSlider.value);
            state.debridService = dom.debridSelect.value;
            state.audioAlerts = dom.audioToggle.checked;
            state.autoTerminate = dom.autoTermToggle.checked;

            // Push settings to API in live mode
            if (isLiveMode) {
                try {
                    await apiFetch('/settings', {
                        method: 'PUT',
                        body: JSON.stringify({ concurrentLimit: state.concurrentLimit }),
                    });
                } catch (e) {
                    showToast('warning', 'API Error', 'Could not save settings to server');
                }
            }

            // Check for new violations with updated limit
            state.sessions.forEach(s => {
                if (state.sessions.indexOf(s) >= state.concurrentLimit && s.status !== 'violation') {
                    s.status = 'violation';
                }
            });

            updateAll();
            closeSettings();
            showToast('success', 'Settings Saved', `Concurrent limit set to ${state.concurrentLimit} streams`);
        });

        // Clear alerts
        dom.clearAlertsBtn.addEventListener('click', () => {
            clearAlerts();
            showToast('info', 'Alerts Cleared', 'All alerts have been dismissed');
        });

        // Range chips
        document.querySelectorAll('.chip[data-range]').forEach(chip => {
            chip.addEventListener('click', () => {
                document.querySelectorAll('.chip[data-range]').forEach(c => c.classList.remove('active'));
                chip.classList.add('active');
                state.selectedRange = chip.dataset.range;
                generateTimelineData();
                drawTimeline();
            });
        });

        // Alerts button scroll to alerts
        dom.alertsBtn.addEventListener('click', () => {
            document.getElementById('alertsCard').scrollIntoView({ behavior: 'smooth', block: 'center' });
        });

        // Window resize
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => {
                drawTimeline();
                drawDonut();
                drawViolationHistory();
            }, 150);
        });
    }

    // ============================================
    // Initialization
    // ============================================
    async function init() {
        cacheDom();

        // Detect whether API is available
        const live = await detectLiveMode();

        if (live) {
            // LIVE MODE: fetch real data from AIOStreams middleware
            await initSessionsLive();
            generateTimelineData();
            generateViolationHistory();

            addAlert('info', 'Dashboard Online', 'Connected to AIOStreams — monitoring live concurrent streams', Date.now());

            updateAll();
            drawViolationHistory();
            bindEvents();
            startPolling();

            setTimeout(() => {
                showToast('success', 'Live Mode', 'Connected to AIOStreams API — real-time monitoring active');
            }, 500);

            // Update connection badge
            dom.connectionStatus.querySelector('.status-text').textContent = 'Live';
        } else {
            // DEMO MODE: simulated data for standalone use
            initSessionsDemo();
            generateTimelineData();
            generateViolationHistory();

            addAlert('info', 'Demo Mode', 'API not available — showing simulated data', Date.now());
            addAlert('warning', 'Approaching Limit', `${state.sessions.length}/${state.concurrentLimit} concurrent streams active`, Date.now() - 60000);

            updateAll();
            drawViolationHistory();
            bindEvents();
            startSimulation();

            setTimeout(() => {
                showToast('info', 'Demo Mode', 'API not available — using simulated data');
            }, 1500);

            // Update connection badge to show demo
            dom.connectionStatus.querySelector('.status-text').textContent = 'Demo';
            dom.connectionStatus.querySelector('.status-dot').classList.remove('live');
            dom.connectionStatus.style.background = 'rgba(251, 191, 36, 0.08)';
            dom.connectionStatus.style.borderColor = 'rgba(251, 191, 36, 0.2)';
            dom.connectionStatus.style.color = '#fbbf24';
        }
    }

    // Boot
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => init());
    } else {
        init();
    }
})();

EOF

cat << 'EOF' > packages/server/src/static/monitor/index.css
/* ============================================
   AIOStreams Monitor — Design System
   ============================================ */

:root {
    /* Color Palette */
    --bg-primary: #0a0a0f;
    --bg-secondary: #12121a;
    --bg-card: rgba(20, 20, 32, 0.72);
    --bg-card-hover: rgba(28, 28, 44, 0.85);
    --bg-glass: rgba(255, 255, 255, 0.03);
    --bg-elevated: rgba(30, 30, 48, 0.9);

    --border-subtle: rgba(255, 255, 255, 0.06);
    --border-active: rgba(99, 102, 241, 0.4);

    --text-primary: #f0f0f5;
    --text-secondary: #8b8ba3;
    --text-tertiary: #5a5a72;
    --text-accent: #a78bfa;

    /* Accent Colors */
    --accent-indigo: #6366f1;
    --accent-violet: #8b5cf6;
    --accent-purple: #a78bfa;
    --accent-cyan: #22d3ee;
    --accent-emerald: #34d399;
    --accent-amber: #fbbf24;
    --accent-rose: #f43f5e;
    --accent-orange: #fb923c;

    /* Gradients */
    --gradient-primary: linear-gradient(135deg, #6366f1, #a78bfa);
    --gradient-danger: linear-gradient(135deg, #f43f5e, #fb7185);
    --gradient-success: linear-gradient(135deg, #34d399, #6ee7b7);
    --gradient-warning: linear-gradient(135deg, #fbbf24, #f59e0b);

    /* Sizing */
    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
    --radius-xl: 20px;
    --radius-full: 9999px;

    /* Shadows */
    --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.3);
    --shadow-md: 0 4px 20px rgba(0, 0, 0, 0.4);
    --shadow-lg: 0 8px 40px rgba(0, 0, 0, 0.5);
    --shadow-glow-indigo: 0 0 30px rgba(99, 102, 241, 0.15);
    --shadow-glow-rose: 0 0 30px rgba(244, 63, 94, 0.15);

    /* Transitions */
    --transition-fast: 150ms cubic-bezier(0.4, 0, 0.2, 1);
    --transition-base: 250ms cubic-bezier(0.4, 0, 0.2, 1);
    --transition-slow: 400ms cubic-bezier(0.4, 0, 0.2, 1);
}

/* ============================================
   Reset & Base
   ============================================ */
*, *::before, *::after {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

html {
    font-size: 15px;
    scroll-behavior: smooth;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    min-height: 100vh;
    overflow-x: hidden;
    line-height: 1.5;
}

/* ============================================
   Ambient Background
   ============================================ */
.ambient-bg {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 0;
    pointer-events: none;
    overflow: hidden;
}

.ambient-orb {
    position: absolute;
    border-radius: 50%;
    filter: blur(100px);
    opacity: 0.25;
}

.ambient-orb-1 {
    width: 600px;
    height: 600px;
    background: radial-gradient(circle, #6366f1, transparent 70%);
    top: -200px;
    right: -100px;
    animation: orbFloat1 20s ease-in-out infinite;
}

.ambient-orb-2 {
    width: 500px;
    height: 500px;
    background: radial-gradient(circle, #a78bfa, transparent 70%);
    bottom: -150px;
    left: -100px;
    animation: orbFloat2 25s ease-in-out infinite;
}

.ambient-orb-3 {
    width: 350px;
    height: 350px;
    background: radial-gradient(circle, #22d3ee, transparent 70%);
    top: 40%;
    left: 50%;
    animation: orbFloat3 18s ease-in-out infinite;
}

@keyframes orbFloat1 {
    0%, 100% { transform: translate(0, 0) scale(1); }
    33% { transform: translate(-60px, 40px) scale(1.1); }
    66% { transform: translate(30px, -30px) scale(0.9); }
}

@keyframes orbFloat2 {
    0%, 100% { transform: translate(0, 0) scale(1); }
    33% { transform: translate(50px, -40px) scale(1.15); }
    66% { transform: translate(-40px, 30px) scale(0.85); }
}

@keyframes orbFloat3 {
    0%, 100% { transform: translate(-50%, 0) scale(1); opacity: 0.15; }
    50% { transform: translate(-50%, -30px) scale(1.2); opacity: 0.25; }
}

/* ============================================
   App Container
   ============================================ */
.app-container {
    position: relative;
    z-index: 1;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
}

/* ============================================
   Top Navigation Bar
   ============================================ */
.top-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 2rem;
    height: 64px;
    background: rgba(10, 10, 15, 0.8);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border-bottom: 1px solid var(--border-subtle);
    position: sticky;
    top: 0;
    z-index: 100;
}

.top-bar-left, .top-bar-right {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.logo {
    display: flex;
    align-items: center;
    gap: 0.6rem;
}

.logo-icon {
    display: flex;
    align-items: center;
    animation: logoPulse 3s ease-in-out infinite;
}

@keyframes logoPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}

.logo-text {
    font-size: 1.15rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    color: var(--text-primary);
}

.logo-accent {
    background: var(--gradient-primary);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: 800;
}

.connection-status {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.3rem 0.75rem;
    background: rgba(52, 211, 153, 0.08);
    border: 1px solid rgba(52, 211, 153, 0.2);
    border-radius: var(--radius-full);
    font-size: 0.75rem;
    font-weight: 500;
    color: var(--accent-emerald);
}

.status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--accent-emerald);
    position: relative;
}

.status-dot.live::after {
    content: '';
    position: absolute;
    inset: -3px;
    border-radius: 50%;
    background: var(--accent-emerald);
    opacity: 0.4;
    animation: dotPing 2s cubic-bezier(0, 0, 0.2, 1) infinite;
}

@keyframes dotPing {
    0% { transform: scale(1); opacity: 0.4; }
    75%, 100% { transform: scale(2); opacity: 0; }
}

.last-updated {
    font-size: 0.75rem;
    color: var(--text-tertiary);
}

.btn-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    border-radius: var(--radius-sm);
    border: 1px solid var(--border-subtle);
    background: var(--bg-glass);
    color: var(--text-secondary);
    cursor: pointer;
    transition: all var(--transition-fast);
    position: relative;
}

.btn-icon:hover {
    background: var(--bg-card-hover);
    color: var(--text-primary);
    border-color: var(--border-active);
}

.alert-badge {
    position: absolute;
    top: -4px;
    right: -4px;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: var(--accent-rose);
    color: white;
    font-size: 0.65rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    justify-content: center;
    animation: badgePop 0.3s ease-out;
}

@keyframes badgePop {
    0% { transform: scale(0); }
    70% { transform: scale(1.2); }
    100% { transform: scale(1); }
}

/* ============================================
   Main Content
   ============================================ */
.main-content {
    padding: 1.5rem 2rem 3rem;
    flex: 1;
}

/* ============================================
   KPI Cards
   ============================================ */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 1.5rem;
}

.kpi-card {
    position: relative;
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.25rem 1.5rem;
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-lg);
    transition: all var(--transition-base);
    overflow: hidden;
}

.kpi-card::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(135deg, rgba(255,255,255,0.02), transparent);
    pointer-events: none;
}

.kpi-card:hover {
    transform: translateY(-2px);
    border-color: var(--border-active);
    box-shadow: var(--shadow-glow-indigo);
}

.kpi-card.kpi-warning:hover {
    box-shadow: var(--shadow-glow-rose);
    border-color: rgba(244, 63, 94, 0.3);
}

.kpi-card.violation-active {
    border-color: rgba(244, 63, 94, 0.5);
    animation: violationPulse 2s ease-in-out infinite;
}

@keyframes violationPulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(244, 63, 94, 0); }
    50% { box-shadow: 0 0 20px 4px rgba(244, 63, 94, 0.15); }
}

.kpi-icon-wrap {
    width: 48px;
    height: 48px;
    border-radius: var(--radius-md);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
}

.kpi-streams { background: rgba(99, 102, 241, 0.12); color: var(--accent-indigo); }
.kpi-limit { background: rgba(34, 211, 238, 0.1); color: var(--accent-cyan); }
.kpi-violations { background: rgba(244, 63, 94, 0.1); color: var(--accent-rose); }
.kpi-ips { background: rgba(251, 191, 36, 0.1); color: var(--accent-amber); }
.kpi-network { background: rgba(52, 211, 153, 0.1); color: #34d399; }

.network-stats {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: var(--text-secondary);
    letter-spacing: 0.02em;
}

.kpi-data {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
}

.kpi-value {
    font-size: 1.75rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    line-height: 1.1;
    font-family: 'JetBrains Mono', 'Inter', monospace;
}

.kpi-label {
    font-size: 0.75rem;
    color: var(--text-secondary);
    font-weight: 500;
    margin-top: 2px;
}

.kpi-trend {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    font-size: 0.75rem;
    font-weight: 600;
    font-family: 'JetBrains Mono', monospace;
    padding: 0.2rem 0.5rem;
    border-radius: var(--radius-full);
}

.kpi-trend.up {
    color: var(--accent-emerald);
    background: rgba(52, 211, 153, 0.1);
}

.kpi-trend.down {
    color: var(--accent-rose);
    background: rgba(244, 63, 94, 0.1);
}

.kpi-capacity {
    width: 60px;
    height: 6px;
    background: rgba(255,255,255,0.06);
    border-radius: var(--radius-full);
    overflow: hidden;
}

.capacity-fill {
    height: 100%;
    border-radius: var(--radius-full);
    background: var(--gradient-primary);
    transition: width var(--transition-slow);
}

.capacity-fill.warning { background: var(--gradient-warning); }
.capacity-fill.danger { background: var(--gradient-danger); }

.kpi-sub {
    font-size: 0.7rem;
    font-weight: 500;
}

.ip-safe { color: var(--accent-emerald); }
.ip-warning { color: var(--accent-amber); }
.ip-danger { color: var(--accent-rose); }

/* ============================================
   Dashboard Grid
   ============================================ */
.dashboard-grid {
    display: grid;
    grid-template-columns: 1fr 320px;
    grid-template-rows: auto auto auto;
    gap: 1rem;
}

/* ============================================
   Card Base
   ============================================ */
.card {
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-lg);
    overflow: hidden;
    transition: border-color var(--transition-base);
}

.card:hover {
    border-color: rgba(99, 102, 241, 0.15);
}

.card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 1.25rem;
    border-bottom: 1px solid var(--border-subtle);
}

.card-title {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--text-primary);
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.card-actions {
    display: flex;
    align-items: center;
    gap: 0.4rem;
}

/* ============================================
   Chips / Pills
   ============================================ */
.chip {
    padding: 0.3rem 0.7rem;
    font-size: 0.7rem;
    font-weight: 600;
    font-family: 'JetBrains Mono', monospace;
    border-radius: var(--radius-full);
    border: 1px solid var(--border-subtle);
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    transition: all var(--transition-fast);
}

.chip:hover {
    background: rgba(99, 102, 241, 0.08);
    border-color: var(--border-active);
    color: var(--text-primary);
}

.chip.active {
    background: rgba(99, 102, 241, 0.15);
    border-color: var(--accent-indigo);
    color: var(--accent-purple);
}

/* ============================================
   Timeline Chart
   ============================================ */
.card-chart {
    grid-column: 1 / 2;
    grid-row: 1 / 2;
}

.chart-container {
    position: relative;
    padding: 1rem 1.25rem 0.75rem;
    height: 240px;
}

.chart-container canvas {
    width: 100% !important;
    height: 100% !important;
}

.chart-limit-line {
    position: absolute;
    left: 1.25rem;
    right: 1.25rem;
    border-top: 2px dashed rgba(244, 63, 94, 0.35);
    pointer-events: none;
    transition: top var(--transition-base);
}

.limit-label {
    position: absolute;
    right: 0;
    top: -10px;
    font-size: 0.65rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    color: var(--accent-rose);
    background: rgba(244, 63, 94, 0.1);
    padding: 0.15rem 0.5rem;
    border-radius: var(--radius-sm);
}

/* ============================================
   Donut Chart
   ============================================ */
.card-donut {
    grid-column: 2 / 3;
    grid-row: 1 / 2;
}

.donut-container {
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 1.25rem 1rem 0.75rem;
}

.donut-center {
    position: absolute;
    display: flex;
    flex-direction: column;
    align-items: center;
}

.donut-value {
    font-size: 1.5rem;
    font-weight: 800;
    font-family: 'JetBrains Mono', monospace;
    color: var(--text-primary);
}

.donut-label {
    font-size: 0.65rem;
    font-weight: 500;
    color: var(--text-tertiary);
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.donut-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    padding: 0.5rem 1rem 1rem;
    justify-content: center;
}

.legend-item {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.7rem;
    color: var(--text-secondary);
    font-weight: 500;
}

.legend-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
}

/* ============================================
   Sessions Table
   ============================================ */
.card-table {
    grid-column: 1 / -1;
    grid-row: 2 / 3;
}

.search-wrap {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.35rem 0.75rem;
    background: rgba(255,255,255,0.04);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-full);
    transition: border-color var(--transition-fast);
}

.search-wrap:focus-within {
    border-color: var(--border-active);
}

.search-wrap svg {
    color: var(--text-tertiary);
    flex-shrink: 0;
}

.search-input {
    border: none;
    background: transparent;
    color: var(--text-primary);
    font-size: 0.8rem;
    font-family: inherit;
    outline: none;
    width: 180px;
}

.search-input::placeholder {
    color: var(--text-tertiary);
}

.table-scroll {
    overflow-x: auto;
}

.sessions-table {
    width: 100%;
    border-collapse: collapse;
}

.sessions-table th {
    text-align: left;
    padding: 0.65rem 1rem;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-tertiary);
    border-bottom: 1px solid var(--border-subtle);
    white-space: nowrap;
}

.sessions-table td {
    padding: 0.75rem 1rem;
    font-size: 0.82rem;
    border-bottom: 1px solid var(--border-subtle);
    vertical-align: middle;
    white-space: nowrap;
}

.sessions-table tbody tr {
    transition: background var(--transition-fast);
}

.sessions-table tbody tr:hover {
    background: rgba(99, 102, 241, 0.04);
}

.sessions-table tbody tr:last-child td {
    border-bottom: none;
}

.session-user {
    display: flex;
    align-items: center;
    gap: 0.6rem;
}

.session-avatar {
    width: 32px;
    height: 32px;
    border-radius: var(--radius-sm);
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 0.75rem;
    flex-shrink: 0;
}

.session-user-info {
    display: flex;
    flex-direction: column;
}

.session-username {
    font-weight: 600;
    font-size: 0.82rem;
    color: var(--text-primary);
}

.session-device {
    font-size: 0.7rem;
    color: var(--text-tertiary);
}

.session-uuid-cell {
    display: flex;
    align-items: center;
    gap: 0.4rem;
}

.session-uuid {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: var(--text-accent);
    background: rgba(167, 139, 250, 0.08);
    padding: 0.2rem 0.5rem;
    border-radius: var(--radius-sm);
    letter-spacing: 0.02em;
    cursor: default;
    border: 1px solid rgba(167, 139, 250, 0.12);
    transition: all var(--transition-fast);
}

.session-uuid:hover {
    background: rgba(167, 139, 250, 0.14);
    border-color: rgba(167, 139, 250, 0.25);
}

.btn-copy-uuid {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    border-radius: var(--radius-sm);
    border: 1px solid transparent;
    background: transparent;
    color: var(--text-tertiary);
    cursor: pointer;
    transition: all var(--transition-fast);
    flex-shrink: 0;
    opacity: 0;
}

.sessions-table tbody tr:hover .btn-copy-uuid {
    opacity: 1;
}

.btn-copy-uuid:hover {
    background: rgba(167, 139, 250, 0.1);
    border-color: rgba(167, 139, 250, 0.25);
    color: var(--text-accent);
}

.btn-copy-uuid:active {
    transform: scale(0.9);
}

.session-ip {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: var(--text-secondary);
}

.session-content {
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
}

.session-duration {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: var(--text-secondary);
}

.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.2rem 0.6rem;
    border-radius: var(--radius-full);
    font-size: 0.7rem;
    font-weight: 600;
}

.status-badge.streaming {
    background: rgba(52, 211, 153, 0.1);
    color: var(--accent-emerald);
}

.status-badge.buffering {
    background: rgba(251, 191, 36, 0.1);
    color: var(--accent-amber);
}

.status-badge.paused {
    background: rgba(139, 92, 246, 0.1);
    color: var(--accent-violet);
}

.status-badge.violation {
    background: rgba(244, 63, 94, 0.12);
    color: var(--accent-rose);
    animation: statusFlash 1.5s ease-in-out infinite;
}

@keyframes statusFlash {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
}

.status-badge-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
}

.btn-terminate {
    padding: 0.3rem 0.65rem;
    font-size: 0.7rem;
    font-weight: 600;
    border-radius: var(--radius-sm);
    border: 1px solid rgba(244, 63, 94, 0.2);
    background: rgba(244, 63, 94, 0.06);
    color: var(--accent-rose);
    cursor: pointer;
    transition: all var(--transition-fast);
    font-family: inherit;
}

.btn-terminate:hover {
    background: rgba(244, 63, 94, 0.15);
    border-color: rgba(244, 63, 94, 0.4);
    transform: scale(1.02);
}

/* ============================================
   Alerts Panel
   ============================================ */
.card-alerts {
    grid-column: 2 / 3;
    grid-row: 3 / 4;
}

.btn-text {
    font-size: 0.72rem;
    font-weight: 500;
    color: var(--text-tertiary);
    background: none;
    border: none;
    cursor: pointer;
    transition: color var(--transition-fast);
    font-family: inherit;
}

.btn-text:hover {
    color: var(--text-primary);
}

.alerts-list {
    max-height: 350px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: rgba(255,255,255,0.1) transparent;
}

.alert-item {
    display: flex;
    gap: 0.75rem;
    padding: 0.85rem 1.25rem;
    border-bottom: 1px solid var(--border-subtle);
    transition: background var(--transition-fast);
    animation: alertSlideIn 0.3s ease-out;
}

@keyframes alertSlideIn {
    from {
        opacity: 0;
        transform: translateX(10px);
    }
    to {
        opacity: 1;
        transform: translateX(0);
    }
}

.alert-item:hover {
    background: rgba(255,255,255,0.02);
}

.alert-item:last-child {
    border-bottom: none;
}

.alert-icon {
    width: 32px;
    height: 32px;
    border-radius: var(--radius-sm);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    font-size: 0.9rem;
}

.alert-icon.critical {
    background: rgba(244, 63, 94, 0.1);
    color: var(--accent-rose);
}

.alert-icon.warning {
    background: rgba(251, 191, 36, 0.1);
    color: var(--accent-amber);
}

.alert-icon.info {
    background: rgba(99, 102, 241, 0.1);
    color: var(--accent-indigo);
}

.alert-icon.success {
    background: rgba(52, 211, 153, 0.1);
    color: var(--accent-emerald);
}

.alert-body {
    flex: 1;
    min-width: 0;
}

.alert-title {
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 0.15rem;
}

.alert-desc {
    font-size: 0.7rem;
    color: var(--text-tertiary);
    line-height: 1.4;
}

.alert-time {
    font-size: 0.65rem;
    color: var(--text-tertiary);
    font-family: 'JetBrains Mono', monospace;
    white-space: nowrap;
    margin-top: 0.2rem;
}

/* ============================================
   Violations Chart
   ============================================ */
.card-violations {
    grid-column: 1 / 2;
    grid-row: 3 / 4;
}

.violations-chart-container {
    padding: 1rem 1.25rem;
    height: 220px;
}

.violations-chart-container canvas {
    width: 100% !important;
    height: 100% !important;
}

/* ============================================
   Modal
   ============================================ */
.modal-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.6);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
    opacity: 0;
    pointer-events: none;
    transition: opacity var(--transition-base);
}

.modal-overlay.active {
    opacity: 1;
    pointer-events: auto;
}

.modal-content {
    width: 90%;
    max-width: 480px;
    background: var(--bg-elevated);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-xl);
    box-shadow: var(--shadow-lg);
    transform: translateY(20px) scale(0.97);
    transition: transform var(--transition-base);
}

.modal-overlay.active .modal-content {
    transform: translateY(0) scale(1);
}

.modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1.25rem 1.5rem;
    border-bottom: 1px solid var(--border-subtle);
}

.modal-header h2 {
    font-size: 1rem;
    font-weight: 700;
}

.modal-close {
    border: none;
    background: none;
}

.modal-body {
    padding: 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
}

.setting-group {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
}

.setting-label {
    font-size: 0.85rem;
    font-weight: 500;
    color: var(--text-secondary);
}

.setting-control {
    display: flex;
    align-items: center;
    gap: 0.75rem;
}

.range-slider {
    width: 120px;
    height: 4px;
    -webkit-appearance: none;
    appearance: none;
    background: rgba(255,255,255,0.1);
    border-radius: var(--radius-full);
    outline: none;
}

.range-slider::-webkit-slider-thumb {
    -webkit-appearance: none;
    width: 16px;
    height: 16px;
    border-radius: 50%;
    background: var(--accent-indigo);
    cursor: pointer;
    border: 2px solid var(--bg-elevated);
    box-shadow: 0 0 10px rgba(99, 102, 241, 0.4);
}

.range-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-accent);
    min-width: 30px;
    text-align: right;
}

.setting-select {
    padding: 0.5rem 0.75rem;
    background: rgba(255,255,255,0.04);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-sm);
    color: var(--text-primary);
    font-family: inherit;
    font-size: 0.82rem;
    outline: none;
    cursor: pointer;
}

.setting-select:focus {
    border-color: var(--border-active);
}

.setting-select option {
    background: var(--bg-secondary);
}

/* Toggle Switch */
.toggle-switch {
    position: relative;
    width: 44px;
    height: 24px;
    cursor: pointer;
}

.toggle-switch input {
    opacity: 0;
    width: 0;
    height: 0;
}

.toggle-slider {
    position: absolute;
    inset: 0;
    background: rgba(255,255,255,0.08);
    border-radius: var(--radius-full);
    transition: all var(--transition-fast);
}

.toggle-slider::before {
    content: '';
    position: absolute;
    left: 2px;
    top: 2px;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    background: var(--text-secondary);
    transition: all var(--transition-fast);
}

.toggle-switch input:checked + .toggle-slider {
    background: rgba(99, 102, 241, 0.3);
}

.toggle-switch input:checked + .toggle-slider::before {
    transform: translateX(20px);
    background: var(--accent-indigo);
}

.modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.75rem;
    padding: 1rem 1.5rem;
    border-top: 1px solid var(--border-subtle);
}

.btn-secondary {
    padding: 0.55rem 1.25rem;
    font-size: 0.82rem;
    font-weight: 600;
    border-radius: var(--radius-sm);
    border: 1px solid var(--border-subtle);
    background: transparent;
    color: var(--text-secondary);
    cursor: pointer;
    font-family: inherit;
    transition: all var(--transition-fast);
}

.btn-secondary:hover {
    background: rgba(255,255,255,0.05);
    color: var(--text-primary);
}

.btn-primary {
    padding: 0.55rem 1.25rem;
    font-size: 0.82rem;
    font-weight: 600;
    border-radius: var(--radius-sm);
    border: none;
    background: var(--gradient-primary);
    color: white;
    cursor: pointer;
    font-family: inherit;
    transition: all var(--transition-fast);
}

.btn-primary:hover {
    opacity: 0.9;
    transform: translateY(-1px);
}

/* ============================================
   Toast Notifications
   ============================================ */
.toast-container {
    position: fixed;
    bottom: 1.5rem;
    right: 1.5rem;
    z-index: 1100;
    display: flex;
    flex-direction: column-reverse;
    gap: 0.5rem;
}

.toast {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.85rem 1.25rem;
    background: var(--bg-elevated);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-md);
    box-shadow: var(--shadow-lg);
    min-width: 300px;
    max-width: 420px;
    animation: toastIn 0.35s ease-out;
    position: relative;
    overflow: hidden;
}

.toast.removing {
    animation: toastOut 0.3s ease-in forwards;
}

@keyframes toastIn {
    from {
        opacity: 0;
        transform: translateX(40px) scale(0.95);
    }
    to {
        opacity: 1;
        transform: translateX(0) scale(1);
    }
}

@keyframes toastOut {
    to {
        opacity: 0;
        transform: translateX(40px) scale(0.95);
    }
}

.toast-progress {
    position: absolute;
    bottom: 0;
    left: 0;
    height: 3px;
    border-radius: 0 0 var(--radius-md) 0;
    animation: toastProgress 4s linear forwards;
}

@keyframes toastProgress {
    from { width: 100%; }
    to { width: 0%; }
}

.toast.critical { border-left: 3px solid var(--accent-rose); }
.toast.critical .toast-progress { background: var(--accent-rose); }
.toast.warning { border-left: 3px solid var(--accent-amber); }
.toast.warning .toast-progress { background: var(--accent-amber); }
.toast.info { border-left: 3px solid var(--accent-indigo); }
.toast.info .toast-progress { background: var(--accent-indigo); }
.toast.success { border-left: 3px solid var(--accent-emerald); }
.toast.success .toast-progress { background: var(--accent-emerald); }

.toast-content {
    flex: 1;
}

.toast-title {
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--text-primary);
}

.toast-message {
    font-size: 0.72rem;
    color: var(--text-tertiary);
    margin-top: 0.1rem;
}

.toast-close {
    background: none;
    border: none;
    color: var(--text-tertiary);
    cursor: pointer;
    padding: 0.2rem;
    transition: color var(--transition-fast);
}

.toast-close:hover {
    color: var(--text-primary);
}

/* ============================================
   Scrollbar
   ============================================ */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}

::-webkit-scrollbar-track {
    background: transparent;
}

::-webkit-scrollbar-thumb {
    background: rgba(255, 255, 255, 0.08);
    border-radius: var(--radius-full);
}

::-webkit-scrollbar-thumb:hover {
    background: rgba(255, 255, 255, 0.15);
}

/* ============================================
   Responsive
   ============================================ */
@media (max-width: 1200px) {
    .kpi-row {
        grid-template-columns: repeat(2, 1fr);
    }

    .dashboard-grid {
        grid-template-columns: 1fr;
    }

    .card-chart { grid-column: 1 / -1; grid-row: auto; }
    .card-donut { grid-column: 1 / -1; grid-row: auto; }
    .card-table { grid-column: 1 / -1; grid-row: auto; }
    .card-violations { grid-column: 1 / -1; grid-row: auto; }
    .card-alerts { grid-column: 1 / -1; grid-row: auto; }
}

@media (max-width: 768px) {
    .kpi-row {
        grid-template-columns: 1fr;
    }

    .top-bar {
        padding: 0 1rem;
    }

    .main-content {
        padding: 1rem;
    }

    .search-input {
        width: 120px;
    }
}

/* ============================================
   Animations — Content Entry
   ============================================ */
.kpi-card, .card {
    animation: cardFadeIn 0.5s ease-out both;
}

.kpi-card:nth-child(1) { animation-delay: 0.05s; }
.kpi-card:nth-child(2) { animation-delay: 0.1s; }
.kpi-card:nth-child(3) { animation-delay: 0.15s; }
.kpi-card:nth-child(4) { animation-delay: 0.2s; }

.card-chart { animation-delay: 0.25s; }
.card-donut { animation-delay: 0.3s; }
.card-table { animation-delay: 0.35s; }
.card-violations { animation-delay: 0.4s; }
.card-alerts { animation-delay: 0.45s; }

@keyframes cardFadeIn {
    from {
        opacity: 0;
        transform: translateY(15px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

/* Empty state */
.empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    color: var(--text-tertiary);
    font-size: 0.85rem;
}

.empty-state svg {
    margin-bottom: 0.75rem;
    opacity: 0.4;
}

EOF

cat << 'EOF' > packages/server/src/static/monitor/index.html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AIOStreams Monitor — Concurrent Stream Violation Dashboard</title>
    <meta name="description" content="Real-time monitoring dashboard for AIOStreams concurrent stream sessions, violations, and debrid service limits.">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="index.css">
</head>
<body>
    <!-- Ambient background effects -->
    <div class="ambient-bg">
        <div class="ambient-orb ambient-orb-1"></div>
        <div class="ambient-orb ambient-orb-2"></div>
        <div class="ambient-orb ambient-orb-3"></div>
    </div>

    <div class="app-container">
        <!-- Top Navigation Bar -->
        <header class="top-bar" id="topBar">
            <div class="top-bar-left">
                <div class="logo">
                    <div class="logo-icon">
                        <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
                            <circle cx="14" cy="14" r="12" stroke="url(#logoGrad)" stroke-width="2.5" fill="none"/>
                            <circle cx="14" cy="14" r="6" fill="url(#logoGrad)" opacity="0.6"/>
                            <circle cx="14" cy="14" r="3" fill="url(#logoGrad)"/>
                            <defs>
                                <linearGradient id="logoGrad" x1="0" y1="0" x2="28" y2="28">
                                    <stop offset="0%" stop-color="#6366f1"/>
                                    <stop offset="100%" stop-color="#a78bfa"/>
                                </linearGradient>
                            </defs>
                        </svg>
                    </div>
                    <span class="logo-text">AIOStreams<span class="logo-accent">Monitor</span></span>
                </div>
                <div class="connection-status" id="connectionStatus">
                    <span class="status-dot live"></span>
                    <span class="status-text">Live</span>
                </div>
            </div>
            <div class="top-bar-right">
                <div class="last-updated" id="lastUpdated">Updated just now</div>
                <button class="btn-icon" id="settingsBtn" title="Settings">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>
                </button>
                <button class="btn-icon" id="alertsBtn" title="Alerts">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
                    <span class="alert-badge" id="alertBadge">3</span>
                </button>
            </div>
        </header>

        <!-- Main Content Area -->
        <main class="main-content">
            <!-- KPI Cards Row -->
            <section class="kpi-row" id="kpiRow">
                <div class="kpi-card" id="kpiActiveStreams">
                    <div class="kpi-icon-wrap kpi-streams">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
                    </div>
                    <div class="kpi-data">
                        <span class="kpi-value" id="activeStreamCount">0</span>
                        <span class="kpi-label">Active Streams</span>
                    </div>
                    <div class="kpi-trend up" id="streamTrend">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>
                        <span id="streamTrendText">+2</span>
                    </div>
                </div>

                <div class="kpi-card" id="kpiConcurrentLimit">
                    <div class="kpi-icon-wrap kpi-limit">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                    </div>
                    <div class="kpi-data">
                        <span class="kpi-value" id="concurrentLimit">4</span>
                        <span class="kpi-label">Concurrent Limit</span>
                    </div>
                    <div class="kpi-capacity" id="capacityBar">
                        <div class="capacity-fill" id="capacityFill" style="width: 0%"></div>
                    </div>
                </div>

                <div class="kpi-card kpi-warning" id="kpiViolations">
                    <div class="kpi-icon-wrap kpi-violations">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                    </div>
                    <div class="kpi-data">
                        <span class="kpi-value" id="violationCount">0</span>
                        <span class="kpi-label">Violations (24h)</span>
                    </div>
                    <div class="kpi-trend down" id="violationTrend">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/></svg>
                        <span id="violationTrendText">-1</span>
                    </div>
                </div>

                <div class="kpi-card" id="kpiUniqueIPs">
                    <div class="kpi-icon-wrap kpi-ips">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
                    </div>
                    <div class="kpi-data">
                        <span class="kpi-value" id="uniqueIPCount">0</span>
                        <span class="kpi-label">Unique IPs</span>
                    </div>
                    <div class="kpi-sub" id="ipWarning">
                        <span class="ip-safe">Within safe range</span>
                    </div>
                </div>

                <div class="kpi-card" id="kpiNetwork">
                    <div class="kpi-icon-wrap kpi-network">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
                    </div>
                    <div class="kpi-data">
                        <span class="kpi-value" id="networkBandwidth" style="font-size: 1.4rem;">0 B/s</span>
                        <span class="kpi-label">Server Bandwidth</span>
                    </div>
                    <div class="kpi-sub" id="networkDetails">
                        <span class="network-stats">↓ 0 B/s &nbsp; ↑ 0 B/s</span>
                    </div>
                </div>
            </section>

            <!-- Charts & Streams Grid -->
            <section class="dashboard-grid">
                <!-- Stream Timeline Chart -->
                <div class="card card-chart" id="timelineCard">
                    <div class="card-header">
                        <h2 class="card-title">Stream Activity Timeline</h2>
                        <div class="card-actions">
                            <button class="chip active" data-range="1h">1H</button>
                            <button class="chip" data-range="6h">6H</button>
                            <button class="chip" data-range="24h">24H</button>
                            <button class="chip" data-range="7d">7D</button>
                        </div>
                    </div>
                    <div class="chart-container" id="timelineChart">
                        <canvas id="timelineCanvas"></canvas>
                        <div class="chart-limit-line" id="limitLine">
                            <span class="limit-label">Limit: <span id="limitLineValue">4</span></span>
                        </div>
                    </div>
                </div>

                <!-- Service Distribution -->
                <div class="card card-donut" id="serviceCard">
                    <div class="card-header">
                        <h2 class="card-title">Service Distribution</h2>
                    </div>
                    <div class="donut-container">
                        <canvas id="donutCanvas" width="200" height="200"></canvas>
                        <div class="donut-center">
                            <span class="donut-value" id="donutTotal">0</span>
                            <span class="donut-label">Total</span>
                        </div>
                    </div>
                    <div class="donut-legend" id="donutLegend"></div>
                </div>

                <!-- Active Sessions Table -->
                <div class="card card-table" id="sessionsCard">
                    <div class="card-header">
                        <h2 class="card-title">Active Sessions</h2>
                        <div class="card-actions">
                            <div class="search-wrap">
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                                <input type="text" placeholder="Search sessions…" id="sessionSearch" class="search-input">
                            </div>
                        </div>
                    </div>
                    <div class="table-scroll" id="sessionsTableScroll">
                        <table class="sessions-table" id="sessionsTable">
                            <thead>
                                <tr>
                                    <th>Session UUID</th>
                                    <th>User / Device</th>
                                    <th>Service</th>
                                    <th>IP Address</th>
                                    <th>Content</th>
                                    <th>Duration</th>
                                    <th>Status</th>
                                    <th></th>
                                </tr>
                            </thead>
                            <tbody id="sessionsBody"></tbody>
                        </table>
                    </div>
                </div>

                <!-- Alerts Panel -->
                <div class="card card-alerts" id="alertsCard">
                    <div class="card-header">
                        <h2 class="card-title">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
                            Recent Alerts
                        </h2>
                        <button class="btn-text" id="clearAlertsBtn">Clear All</button>
                    </div>
                    <div class="alerts-list" id="alertsList"></div>
                </div>

                <!-- Violation History -->
                <div class="card card-violations" id="violationHistoryCard">
                    <div class="card-header">
                        <h2 class="card-title">Violation History</h2>
                    </div>
                    <div class="violations-chart-container">
                        <canvas id="violationsCanvas"></canvas>
                    </div>
                </div>
            </section>
        </main>
    </div>

    <!-- Settings Modal -->
    <div class="modal-overlay" id="settingsModal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>Dashboard Settings</h2>
                <button class="btn-icon modal-close" id="settingsClose">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                </button>
            </div>
            <div class="modal-body">
                <div class="setting-group">
                    <label class="setting-label">Concurrent Stream Limit</label>
                    <div class="setting-control">
                        <input type="range" id="limitSlider" min="1" max="10" value="4" class="range-slider">
                        <span class="range-value" id="limitDisplay">4</span>
                    </div>
                </div>
                <div class="setting-group">
                    <label class="setting-label">Alert Threshold (%)</label>
                    <div class="setting-control">
                        <input type="range" id="thresholdSlider" min="50" max="100" value="75" class="range-slider">
                        <span class="range-value" id="thresholdDisplay">75%</span>
                    </div>
                </div>
                <div class="setting-group">
                    <label class="setting-label">Debrid Service</label>
                    <select id="debridSelect" class="setting-select">
                        <option value="realdebrid">Real-Debrid</option>
                        <option value="alldebrid">AllDebrid</option>
                        <option value="premiumize">Premiumize</option>
                        <option value="debridlink">Debrid-Link</option>
                        <option value="torbox">TorBox</option>
                    </select>
                </div>
                <div class="setting-group">
                    <label class="setting-label">Audio Alerts</label>
                    <label class="toggle-switch">
                        <input type="checkbox" id="audioToggle" checked>
                        <span class="toggle-slider"></span>
                    </label>
                </div>
                <div class="setting-group">
                    <label class="setting-label">Auto-terminate on violation</label>
                    <label class="toggle-switch">
                        <input type="checkbox" id="autoTermToggle">
                        <span class="toggle-slider"></span>
                    </label>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn-secondary" id="settingsCancel">Cancel</button>
                <button class="btn-primary" id="settingsSave">Save Settings</button>
            </div>
        </div>
    </div>

    <!-- Toast Container -->
    <div class="toast-container" id="toastContainer"></div>

    <script src="app.js"></script>
</body>
</html>

EOF

echo "=== [3/6] Patching Server Source Code ==="

# Patch middlewares/index.ts
echo "export * from './stream-monitor.js';" >> packages/server/src/middlewares/index.ts

# Patch app.ts - Add imports
sed -i "s/requireSessionIfAuthRequired,/requireSessionIfAuthRequired,\\n  streamMonitorMiddleware,\\n  createMonitorRouter,/" packages/server/src/app.ts

# Patch app.ts - Add streamMonitorMiddleware after loggerMiddleware
sed -i "s/app.use(loggerMiddleware);/app.use(loggerMiddleware);\\napp.use(streamMonitorMiddleware);/" packages/server/src/app.ts

# Patch app.ts - Mount /monitor router before Stremio Routes
sed -i "s/\\/\\/ Stremio Routes/\\/\\/ Stream Monitor Dashboard\\napp.use('\\/monitor', createMonitorRouter());\\n\\n\\/\\/ Stremio Routes/" packages/server/src/app.ts

# Patch Dockerfile to copy static files
sed -i "s/COPY packages\\/server \\.\\/packages\\/server/COPY packages\\/server \\.\\/packages\\/server\\nCOPY packages\\/server\\/src\\/static\\/monitor \\.\\/packages\\/server\\/src\\/static\\/monitor/" Dockerfile

echo "=== [4/6] Building Custom Docker Image (this may take a few minutes on ARM64) ==="
docker build -t aiostreams-monitor:latest .

cd ..
rm -rf "$BUILD_DIR"

echo "=== [5/6] Updating docker-compose.yml ==="
if [ -f "docker-compose.yml" ]; then
    sed -i 's/image: ghcr.io\\/viren070\\/aiostreams:latest/image: aiostreams-monitor:latest/' docker-compose.yml
    echo "docker-compose.yml updated successfully."
else
    echo "Warning: docker-compose.yml not found in current directory. Please update it manually."
fi

# Add Env vars to .env.aiostreams if not present
if [ -f ".env.aiostreams" ]; then
    if ! grep -q "MONITOR_CONCURRENT_LIMIT" .env.aiostreams; then
        echo "" >> .env.aiostreams
        echo "# Monitor Settings" >> .env.aiostreams
        echo "MONITOR_CONCURRENT_LIMIT=4" >> .env.aiostreams
        echo "MONITOR_SESSION_TIMEOUT=300000" >> .env.aiostreams
    fi
fi

echo "=== [6/6] Restarting AIOStreams Container ==="
if command -v docker-compose &> /dev/null; then
    docker-compose up -d aiostreams
elif docker compose version &> /dev/null; then
    docker compose up -d aiostreams
else
    echo "Docker compose not found, please start the container manually."
fi

echo ""
echo "============================================================"
echo "âœ… Installation Complete!"
echo "The monitor is now available at: /monitor"
echo "Note: If using Nginx Proxy Manager, don't forget to protect"
echo "the /monitor path with Basic Auth or Access Lists."
echo "============================================================"