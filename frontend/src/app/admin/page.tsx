"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { AuthGuard, AppShell, Button } from "@/components";
import { api } from "@/lib";

// CRM admin panel — only visible to users with is_crm_admin flag
// (granted via PostForge admin panel). Backend enforces the same
// check on every endpoint, so this page is useless without the flag.

type AdminStats = {
  staff: { total_active: number; crm_admins: number };
  accounts: { total_active: number; currently_connected: number };
  contacts: { total: number };
  messages: { total: number; last_24h: number; last_7d: number };
  broadcasts: { running: number; completed_24h: number };
  audit: { events_24h: number };
};

type AuditEntry = {
  id: string;
  action: string;
  actor_id: string;
  actor_name: string;
  actor_org: string | null;
  target_id: string | null;
  target_type: string | null;
  target_contact_id: string | null;
  metadata: Record<string, unknown> | null;
  ip_address: string | null;
  created_at: string;
};

type StaffEntry = {
  id: string;
  name: string;
  role: string;
  org_id: string | null;
  postforge_user_id: string | null;
  is_crm_admin: boolean;
  created_at: string | null;
};

type AccountEntry = {
  id: string;
  phone: string;
  display_name: string | null;
  org_id: string | null;
  connected: boolean;
  created_at: string | null;
  disconnected_at: string | null;
};

export default function AdminPage() {
  return (
    <AuthGuard>
      <AppShell>
        <AdminContent />
      </AppShell>
    </AuthGuard>
  );
}

function AdminContent() {
  const router = useRouter();
  const [tab, setTab] = useState<"stats" | "audit" | "staff" | "accounts">("stats");
  const [accessChecked, setAccessChecked] = useState(false);
  const [hasAccess, setHasAccess] = useState(false);

  useEffect(() => {
    // Verify admin access server-side. If the user doesn't have
    // is_crm_admin, the endpoint returns 403 and we bounce them home.
    api("/api/admin/me")
      .then(() => {
        setHasAccess(true);
        setAccessChecked(true);
      })
      .catch(() => {
        setAccessChecked(true);
        setHasAccess(false);
        router.replace("/chats");
      });
  }, [router]);

  if (!accessChecked) {
    return <div className="p-6 text-center text-slate-400">Проверка доступа...</div>;
  }

  if (!hasAccess) {
    return <div className="p-6 text-center text-red-400">Нет доступа</div>;
  }

  return (
    <div className="p-4 md:p-6 max-w-6xl mx-auto space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-bold flex items-center gap-2">
          <svg className="w-6 h-6 text-amber-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
          </svg>
          CRM Admin Panel
        </h1>
        <span className="text-xs text-amber-400/70 px-2 py-1 rounded bg-amber-500/10 border border-amber-500/20">
          Super-admin access
        </span>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-surface-border overflow-x-auto">
        {([
          { id: "stats", label: "Статистика" },
          { id: "audit", label: "Audit Log" },
          { id: "staff", label: "Сотрудники" },
          { id: "accounts", label: "TG Аккаунты" },
        ] as const).map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              tab === t.id
                ? "border-amber-400 text-amber-400"
                : "border-transparent text-slate-500 hover:text-slate-300"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "stats" && <StatsTab />}
      {tab === "audit" && <AuditTab />}
      {tab === "staff" && <StaffTab />}
      {tab === "accounts" && <AccountsTab />}
    </div>
  );
}

function StatsTab() {
  const [stats, setStats] = useState<AdminStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api("/api/admin/stats")
      .then((data) => setStats(data))
      .catch((e) => console.error("Failed to load stats:", e))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-slate-500 text-sm">Загрузка...</div>;
  if (!stats) return <div className="text-red-400 text-sm">Не удалось загрузить статистику</div>;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      <StatCard
        title="Сотрудники"
        value={stats.staff.total_active}
        subtitle={`${stats.staff.crm_admins} CRM админов`}
        color="blue"
      />
      <StatCard
        title="TG Аккаунты"
        value={`${stats.accounts.currently_connected}/${stats.accounts.total_active}`}
        subtitle="подключено / всего"
        color={stats.accounts.currently_connected === stats.accounts.total_active ? "emerald" : "amber"}
      />
      <StatCard
        title="Контакты"
        value={stats.contacts.total.toLocaleString("ru-RU")}
        subtitle="всего"
        color="violet"
      />
      <StatCard
        title="Сообщения 24ч"
        value={stats.messages.last_24h.toLocaleString("ru-RU")}
        subtitle={`${stats.messages.last_7d.toLocaleString("ru-RU")} за 7 дней`}
        color="cyan"
      />
      <StatCard
        title="Рассылки активные"
        value={stats.broadcasts.running}
        subtitle={`${stats.broadcasts.completed_24h} завершено за 24ч`}
        color={stats.broadcasts.running > 0 ? "emerald" : "slate"}
      />
      <StatCard
        title="Audit события 24ч"
        value={stats.audit.events_24h.toLocaleString("ru-RU")}
        subtitle="в логе за сутки"
        color="amber"
      />
      <StatCard
        title="Всего сообщений"
        value={stats.messages.total.toLocaleString("ru-RU")}
        subtitle="в базе"
        color="slate"
      />
    </div>
  );
}

function StatCard({
  title,
  value,
  subtitle,
  color,
}: {
  title: string;
  value: string | number;
  subtitle: string;
  color: "blue" | "emerald" | "amber" | "violet" | "cyan" | "slate";
}) {
  const colorClasses = {
    blue: "text-blue-400 border-blue-500/20",
    emerald: "text-emerald-400 border-emerald-500/20",
    amber: "text-amber-400 border-amber-500/20",
    violet: "text-violet-400 border-violet-500/20",
    cyan: "text-cyan-400 border-cyan-500/20",
    slate: "text-slate-400 border-slate-500/20",
  };
  return (
    <div className={`bg-gradient-to-br from-surface-card to-surface border ${colorClasses[color]} rounded-xl p-4`}>
      <div className="text-xs text-slate-500 uppercase tracking-wide mb-1">{title}</div>
      <div className={`text-3xl font-bold ${colorClasses[color].split(" ")[0]}`}>{value}</div>
      <div className="text-xs text-slate-500 mt-1">{subtitle}</div>
    </div>
  );
}

function AuditTab() {
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [actions, setActions] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionFilter, setActionFilter] = useState("");
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const LIMIT = 50;

  const load = useCallback(() => {
    setLoading(true);
    const params = new URLSearchParams({ limit: String(LIMIT), offset: String(offset) });
    if (actionFilter) params.set("action", actionFilter);
    api(`/api/admin/audit?${params}`)
      .then((data) => {
        setEntries(data.entries);
        setTotal(data.total);
      })
      .catch((e) => console.error("Failed to load audit:", e))
      .finally(() => setLoading(false));
  }, [actionFilter, offset]);

  useEffect(() => {
    api("/api/admin/audit/actions").then((data) => setActions(data.actions)).catch(() => {});
  }, []);

  useEffect(() => { load(); }, [load]);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <select
          value={actionFilter}
          onChange={(e) => { setActionFilter(e.target.value); setOffset(0); }}
          className="bg-surface border border-surface-border rounded-lg px-3 py-1.5 text-sm text-slate-300"
        >
          <option value="">Все действия</option>
          {actions.map((a) => (
            <option key={a} value={a}>{a}</option>
          ))}
        </select>
        <span className="text-xs text-slate-500">Всего: {total}</span>
      </div>

      {loading ? (
        <div className="text-slate-500 text-sm">Загрузка...</div>
      ) : (
        <div className="bg-surface-card border border-surface-border rounded-xl overflow-hidden">
          <table className="w-full text-xs">
            <thead className="bg-surface/50 text-slate-500">
              <tr>
                <th className="px-3 py-2 text-left font-medium">Время</th>
                <th className="px-3 py-2 text-left font-medium">Действие</th>
                <th className="px-3 py-2 text-left font-medium">Кто</th>
                <th className="px-3 py-2 text-left font-medium">Org</th>
                <th className="px-3 py-2 text-left font-medium">Target</th>
                <th className="px-3 py-2 text-left font-medium">IP</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {entries.map((e) => (
                <tr key={e.id} className="hover:bg-surface/30">
                  <td className="px-3 py-2 text-slate-400 whitespace-nowrap">
                    {new Date(e.created_at).toLocaleString("ru-RU", { dateStyle: "short", timeStyle: "medium" })}
                  </td>
                  <td className="px-3 py-2">
                    <span className="px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 font-mono text-[10px]">
                      {e.action}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-slate-300">{e.actor_name}</td>
                  <td className="px-3 py-2 text-slate-500 font-mono text-[10px] max-w-[140px] truncate" title={e.actor_org || ""}>
                    {e.actor_org ? (e.actor_org.startsWith("personal_") ? "personal" : e.actor_org.slice(0, 8)) : "—"}
                  </td>
                  <td className="px-3 py-2 text-slate-400 max-w-[200px] truncate">
                    {e.target_type && <span className="text-slate-600">{e.target_type}: </span>}
                    {e.target_id || e.target_contact_id || (e.metadata ? JSON.stringify(e.metadata).slice(0, 50) : "—")}
                  </td>
                  <td className="px-3 py-2 text-slate-500 font-mono text-[10px]">{e.ip_address || "—"}</td>
                </tr>
              ))}
              {entries.length === 0 && (
                <tr><td colSpan={6} className="px-3 py-6 text-center text-slate-500">Нет записей</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      <div className="flex items-center justify-between">
        <Button
          variant="ghost"
          onClick={() => setOffset(Math.max(0, offset - LIMIT))}
          disabled={offset === 0}
        >
          ← Назад
        </Button>
        <span className="text-xs text-slate-500">
          {offset + 1}–{Math.min(offset + LIMIT, total)} из {total}
        </span>
        <Button
          variant="ghost"
          onClick={() => setOffset(offset + LIMIT)}
          disabled={offset + LIMIT >= total}
        >
          Вперёд →
        </Button>
      </div>
    </div>
  );
}

function StaffTab() {
  const [staff, setStaff] = useState<StaffEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api("/api/admin/staff")
      .then((data) => setStaff(data.staff))
      .catch((e) => console.error("Failed to load staff:", e))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-slate-500 text-sm">Загрузка...</div>;

  return (
    <div className="bg-surface-card border border-surface-border rounded-xl overflow-hidden">
      <table className="w-full text-xs">
        <thead className="bg-surface/50 text-slate-500">
          <tr>
            <th className="px-3 py-2 text-left font-medium">Имя</th>
            <th className="px-3 py-2 text-left font-medium">Роль</th>
            <th className="px-3 py-2 text-left font-medium">Org</th>
            <th className="px-3 py-2 text-left font-medium">Admin</th>
            <th className="px-3 py-2 text-left font-medium">Создан</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {staff.map((s) => (
            <tr key={s.id} className="hover:bg-surface/30">
              <td className="px-3 py-2 text-slate-200 font-medium">{s.name}</td>
              <td className="px-3 py-2">
                <span className="px-2 py-0.5 rounded bg-slate-500/10 text-slate-400 text-[10px]">{s.role}</span>
              </td>
              <td className="px-3 py-2 text-slate-500 font-mono text-[10px] max-w-[140px] truncate" title={s.org_id || ""}>
                {s.org_id ? (s.org_id.startsWith("personal_") ? "personal" : s.org_id.slice(0, 8)) : "—"}
              </td>
              <td className="px-3 py-2">
                {s.is_crm_admin && (
                  <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 text-[10px] border border-amber-500/40">
                    CRM ADMIN
                  </span>
                )}
              </td>
              <td className="px-3 py-2 text-slate-500 whitespace-nowrap">
                {s.created_at ? new Date(s.created_at).toLocaleDateString("ru-RU") : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AccountsTab() {
  const [accounts, setAccounts] = useState<AccountEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api("/api/admin/accounts")
      .then((data) => setAccounts(data.accounts))
      .catch((e) => console.error("Failed to load accounts:", e))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-slate-500 text-sm">Загрузка...</div>;

  return (
    <div className="bg-surface-card border border-surface-border rounded-xl overflow-hidden">
      <table className="w-full text-xs">
        <thead className="bg-surface/50 text-slate-500">
          <tr>
            <th className="px-3 py-2 text-left font-medium">Телефон</th>
            <th className="px-3 py-2 text-left font-medium">Имя</th>
            <th className="px-3 py-2 text-left font-medium">Org</th>
            <th className="px-3 py-2 text-left font-medium">Статус</th>
            <th className="px-3 py-2 text-left font-medium">Подключён</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {accounts.map((a) => (
            <tr key={a.id} className="hover:bg-surface/30">
              <td className="px-3 py-2 text-slate-200 font-mono">{a.phone}</td>
              <td className="px-3 py-2 text-slate-300">{a.display_name || "—"}</td>
              <td className="px-3 py-2 text-slate-500 font-mono text-[10px] max-w-[140px] truncate" title={a.org_id || ""}>
                {a.org_id ? (a.org_id.startsWith("personal_") ? "personal" : a.org_id.slice(0, 8)) : "—"}
              </td>
              <td className="px-3 py-2">
                <span className={`px-2 py-0.5 rounded text-[10px] ${
                  a.connected
                    ? "bg-emerald-500/10 text-emerald-400"
                    : "bg-red-500/10 text-red-400"
                }`}>
                  {a.connected ? "Подключён" : "Отключён"}
                </span>
              </td>
              <td className="px-3 py-2 text-slate-500 whitespace-nowrap">
                {a.created_at ? new Date(a.created_at).toLocaleDateString("ru-RU") : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
