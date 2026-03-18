"use client";

import { useEffect, useState } from "react";
import { api, type StaffMember, type TgAccount, getTokens } from "@/lib";
import { AppShell, AuthGuard, Badge, Button } from "@/components";

export default function TeamPage() {
  return (
    <AuthGuard>
      <AppShell>
        <TeamContent />
      </AppShell>
    </AuthGuard>
  );
}

function TeamContent() {
  const [staff, setStaff] = useState<StaffMember[]>([]);
  const [accounts, setAccounts] = useState<TgAccount[]>([]);
  const [editingId, setEditingId] = useState<string | null>(null);
  const myRole = getTokens()?.role || "operator";
  const isAdmin = myRole === "super_admin" || myRole === "admin";
  const isSuperAdmin = myRole === "super_admin";

  useEffect(() => {
    api("/api/staff").then(setStaff).catch(console.error);
    if (isAdmin) api("/api/tg/status").then(setAccounts).catch(console.error);
  }, []);

  const toggleActive = async (member: StaffMember) => {
    try {
      const updated = await api(`/api/staff/${member.id}`, {
        method: "PATCH",
        body: JSON.stringify({ is_active: !member.is_active }),
      });
      setStaff((prev) => prev.map((s) => (s.id === member.id ? updated : s)));
    } catch (e: any) { alert(e.message); }
  };

  const changeRole = async (member: StaffMember, newRole: string) => {
    try {
      const updated = await api(`/api/staff/${member.id}`, {
        method: "PATCH",
        body: JSON.stringify({ role: newRole }),
      });
      setStaff((prev) => prev.map((s) => (s.id === member.id ? updated : s)));
    } catch (e: any) { alert(e.message); }
  };

  // Determine if current user can manage this member
  const canManage = (member: StaffMember) => {
    if (member.role === "super_admin") return false; // nobody manages super_admin
    if (isSuperAdmin) return true; // super_admin manages everyone
    if (myRole === "admin" && member.role === "operator") return true; // admin manages operators
    return false;
  };

  const roleColor: Record<string, string> = {
    super_admin: "#ef4444",
    admin: "#f59e0b",
    operator: "#0ea5e9",
  };

  const roleLabel: Record<string, string> = {
    super_admin: "Супер-админ",
    admin: "Админ",
    operator: "Оператор",
  };

  return (
    <div className="p-6 max-w-3xl mx-auto">
      <h1 className="text-2xl font-bold bg-gradient-to-r from-brand to-accent bg-clip-text text-transparent mb-6">
        Команда
      </h1>

      <div className="space-y-3">
        {staff.map((member) => (
          <div
            key={member.id}
            className={`bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-4 animate-fade-in ${
              !member.is_active ? "opacity-40" : ""
            }`}
          >
            <div>
              <div className="flex items-center gap-2 flex-wrap">
                <span className="font-semibold">{member.name}</span>
                <Badge text={roleLabel[member.role] || member.role} color={roleColor[member.role] || "#0ea5e9"} />
                {!member.is_active && (
                  <span className="text-xs text-red-400/60">заблокирован</span>
                )}
              </div>
              <div className="text-xs text-slate-500 mt-1">
                {member.tg_username ? `@${member.tg_username}` : `ID: ${member.tg_user_id}`}
              </div>

              {canManage(member) && (
                <div className="flex gap-2 mt-2 flex-wrap items-center">
                  {/* Role selector — only super_admin can promote to admin */}
                  {isSuperAdmin && (
                    <select
                      value={member.role}
                      onChange={(e) => changeRole(member, e.target.value)}
                      className="px-2 py-1.5 rounded-lg border border-surface-border bg-surface text-xs text-slate-300 focus:outline-none focus:border-brand/30"
                    >
                      <option value="operator">Оператор</option>
                      <option value="admin">Админ</option>
                    </select>
                  )}
                  {accounts.length > 0 && (
                    <Button
                      variant="ghost"
                      onClick={() => setEditingId(editingId === member.id ? null : member.id)}
                    >
                      <svg className="w-4 h-4 inline mr-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
                        <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
                      </svg>
                      Аккаунты
                    </Button>
                  )}
                  <Button
                    variant={member.is_active ? "danger" : "secondary"}
                    onClick={() => toggleActive(member)}
                  >
                    {member.is_active ? "Блокировать" : "Активировать"}
                  </Button>
                </div>
              )}
            </div>

            {editingId === member.id && (
              <AccountAssigner staffId={member.id} accounts={accounts} />
            )}
          </div>
        ))}

        {staff.length === 0 && (
          <div className="text-center text-slate-500 py-12">
            Нет участников в команде
          </div>
        )}
      </div>
    </div>
  );
}

function AccountAssigner({ staffId, accounts }: { staffId: string; accounts: TgAccount[] }) {
  const [assigned, setAssigned] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api(`/api/staff/${staffId}/accounts`)
      .then(setAssigned)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [staffId]);

  const toggle = async (accountId: string) => {
    const next = assigned.includes(accountId)
      ? assigned.filter((a) => a !== accountId)
      : [...assigned, accountId];

    try {
      await api(`/api/staff/${staffId}/accounts`, {
        method: "PUT",
        body: JSON.stringify(next),
      });
      setAssigned(next);
    } catch (e: any) { alert(e.message); }
  };

  if (loading) return <div className="text-xs text-slate-500 mt-3">Загрузка...</div>;

  return (
    <div className="mt-4 pt-4 border-t border-surface-border animate-slide-up">
      <p className="text-xs text-slate-400 mb-2 font-medium">Назначенные TG аккаунты:</p>
      <div className="flex flex-wrap gap-2">
        {accounts.filter((a) => a.is_active).map((acc) => (
          <button
            key={acc.id}
            onClick={() => toggle(acc.id)}
            className={`px-3 py-1.5 rounded-xl text-sm border transition-all duration-200 ${
              assigned.includes(acc.id)
                ? "bg-brand/10 border-brand/30 text-brand shadow-[0_0_10px_rgba(14,165,233,0.1)]"
                : "bg-surface border-surface-border text-slate-400 hover:border-slate-600"
            }`}
          >
            {acc.phone}
          </button>
        ))}
        {accounts.filter((a) => a.is_active).length === 0 && (
          <span className="text-xs text-slate-500">Нет активных TG аккаунтов</span>
        )}
      </div>
    </div>
  );
}
