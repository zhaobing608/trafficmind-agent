import { useState } from 'react';
import type { ReactNode } from 'react';
import {
  AlertOutlined,
  ApartmentOutlined,
  BarChartOutlined,
  BookOutlined,
  CarOutlined,
  CompassOutlined,
  DashboardOutlined,
  DownOutlined,
  FileTextOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  PlusOutlined,
  RightOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import { visualTokens } from '../styles/visualTokens';
import RecentJudgments, { type RecentJudgmentProps } from './collaboration/RecentJudgments';

const { color, radius, shadow } = visualTokens;

const NAV_GROUPS = [
  {
    title: '核心工作台',
    items: [
      { key: 'simulation', label: '交通态势', icon: <CarOutlined /> },
      { key: 'multi', label: '协同研判', icon: <TeamOutlined /> },
    ],
  },
  {
    title: '执行处置',
    items: [
      { key: 'planning', label: '处置方案', icon: <CompassOutlined /> },
      { key: 'workflow', label: '工作流', icon: <ApartmentOutlined /> },
      { key: 'operations', label: '运行监控', icon: <DashboardOutlined /> },
    ],
  },
  {
    title: '决策支撑',
    items: [
      { key: 'alert', label: '风险提醒', icon: <AlertOutlined /> },
      { key: 'report', label: '统计报告', icon: <BarChartOutlined /> },
      { key: 'qa', label: '知识库', icon: <BookOutlined /> },
    ],
  },
];

const UTILITY_ITEMS = [
  { key: 'guide', label: '文档指南', icon: <FileTextOutlined /> },
];

interface RecentItem { id: string; title: string; mode: string; updatedAt: number }

interface Props extends RecentJudgmentProps {
  collapsed: boolean; onToggle: () => void;
  onNavigate: (view: string) => void; onRecentClick: (id: string) => void;
  onNewConversation: () => void; onRenameSession: (id: string, newTitle: string) => void;
  onDeleteSession: (id: string) => void;
  activeView: string; activeConvId?: string; recentList: RecentItem[];
}

export default function Sidebar({ collapsed, onToggle, onNavigate, onRecentClick, onNewConversation, onRenameSession, onDeleteSession, activeView, activeConvId, recentList, ...judgments }: Props) {
  const [recentOpen, setRecentOpen] = useState(true);

  const renderNavItem = (item: { key: string; label: string; icon: ReactNode }, compact = false) => {
    const active = activeView === item.key;
    return (
      <button
        key={item.key}
        data-nav={item.key}
        onClick={() => onNavigate(item.key)}
        title={compact ? item.label : undefined}
        className={`tm-nav-item${active ? ' is-active' : ''}`}
        style={{
          width: '100%',
          display: 'flex',
          position: 'relative',
          alignItems: 'center',
          justifyContent: compact ? 'center' : 'flex-start',
          gap: 9,
          padding: compact ? '10px 0' : '8px 10px 8px 12px',
          borderRadius: radius.md,
          cursor: 'pointer',
          fontSize: 13,
          color: active ? color.primary : '#334155',
          background: active ? color.primarySoft : 'transparent',
          border: '1px solid transparent',
          fontWeight: active ? 650 : 500,
          textAlign: 'left',
          transition: 'background 140ms ease, border-color 140ms ease, color 140ms ease',
        }}
      >
        <span style={{ fontSize: 16, lineHeight: 1, color: active ? color.primary : color.textMuted }}>{item.icon}</span>
        {!compact && <span>{item.label}</span>}
      </button>
    );
  };

  return (
    <>
    <style>{`
      .tm-nav-item:hover {
        background: #F7F9F9 !important;
        border-color: #EEF1F5 !important;
        color: #0F766E !important;
      }
      .tm-nav-item.is-active::before {
        content: '';
        position: absolute;
        left: 5px;
        top: 8px;
        bottom: 8px;
        width: 2px;
        border-radius: 999px;
        background: #0F766E;
      }
      .tm-recent-item:hover {
        background: #F8FAFA !important;
        border-color: #EEF1F5 !important;
      }
    `}</style>
    <div style={{ position: 'fixed', left: 0, top: 0, bottom: 0, width: collapsed ? 72 : 248, background: color.surface, borderRight: `1px solid ${color.borderSubtle}`, display: 'flex', flexDirection: 'column', transition: 'width 0.2s ease', zIndex: 100, overflow: 'hidden', boxShadow: shadow.sidebar }}>
      <div style={{ padding: collapsed ? '12px' : '15px 16px 13px', borderBottom: `1px solid ${color.borderSubtle}`, flexShrink: 0, background: color.surface }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ width: 30, height: 30, borderRadius: radius.md, background: color.primarySoft, border: `1px solid ${color.primaryBorder}`, display: 'flex', alignItems: 'center', justifyContent: 'center', color: color.primary, fontWeight: 750, fontSize: 13, flexShrink: 0 }}>T</div>
          {!collapsed && (
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 15, fontWeight: 700, color: color.text, lineHeight: 1.2 }}>TrafficMind</div>
              <div style={{ fontSize: 11, color: color.textMuted, marginTop: 2 }}>智能交通研判</div>
            </div>
          )}
        </div>
      </div>

      {!collapsed && (
        <div style={{ padding: '12px 12px 8px', flexShrink: 0 }}>
          <button onClick={onNewConversation} style={{ width: '100%', border: `1px solid ${color.border}`, borderRadius: radius.md, padding: '8px 0', background: color.surface, cursor: 'pointer', fontSize: 12, color: color.text, fontWeight: 600, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
            <PlusOutlined /> 新建研判
          </button>
        </div>
      )}
      {collapsed && (
        <div style={{ padding: '10px 12px 6px', flexShrink: 0 }}>
          <button onClick={onNewConversation} title="新建研判" style={{ width: 48, height: 34, border: `1px solid ${color.border}`, borderRadius: radius.md, background: color.surface, cursor: 'pointer', color: color.text }}>
            <PlusOutlined />
          </button>
        </div>
      )}

      {!collapsed && (
        <div style={{ padding: '0 10px 8px', borderBottom: `1px solid ${color.borderSubtle}`, flexShrink: 0 }}>
          {NAV_GROUPS.map(group => (
            <div key={group.title} style={{ marginBottom: 10 }}>
              <div style={{ padding: '0 8px 5px', fontSize: 10, color: color.textSubtle, fontWeight: 600 }}>{group.title}</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                {group.items.map(item => renderNavItem(item))}
              </div>
            </div>
          ))}
        </div>
      )}

      {collapsed && (
        <div style={{ padding: '4px 12px', display: 'flex', flexDirection: 'column', gap: 4, flex: 1 }}>
          {NAV_GROUPS.flatMap(group => group.items).map(item => renderNavItem(item, true))}
        </div>
      )}

      {!collapsed && (
        <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          <button onClick={() => setRecentOpen(!recentOpen)} style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '9px 16px 7px', cursor: 'pointer', fontSize: 11, color: color.textMuted, fontWeight: 700, background: color.surface, border: 'none', borderBottom: recentOpen ? `1px solid ${color.borderSubtle}` : 'none' }}>
            <span>最近研判</span>
            <span style={{ display: 'flex', alignItems: 'center', gap: 6, color: color.textSubtle, fontWeight: 500 }}>
              {recentOpen ? <DownOutlined style={{ fontSize: 8 }} /> : <RightOutlined style={{ fontSize: 8 }} />}
            </span>
          </button>
          {recentOpen && (
            <div style={{ flex: 1, overflowY: 'auto', padding: '6px 10px 10px' }}>
              <RecentJudgments {...judgments} onRecentClick={onRecentClick} />
            </div>
          )}
        </div>
      )}

      {!collapsed && (
        <div style={{ flexShrink: 0, borderTop: `1px solid ${color.borderSubtle}`, padding: '8px 10px' }}>
          {UTILITY_ITEMS.map(item => renderNavItem(item))}
        </div>
      )}
      {collapsed && (
        <div style={{ flexShrink: 0, padding: '6px 12px', borderTop: `1px solid ${color.borderSubtle}` }}>
          {UTILITY_ITEMS.map(item => renderNavItem(item, true))}
        </div>
      )}

      <button type="button" aria-label={collapsed ? '展开侧栏' : '折叠侧栏'} title={collapsed ? '展开侧栏' : '折叠侧栏'} onClick={onToggle} style={{ padding: '8px', textAlign: 'center', cursor: 'pointer', border: 0, background: color.surface, borderTop: `1px solid ${color.borderSubtle}`, color: color.textSubtle, fontSize: 14, flexShrink: 0 }}>
        {collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
      </button>
    </div>
    </>
  );
}
