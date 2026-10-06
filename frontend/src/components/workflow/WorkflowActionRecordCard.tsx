/** Reliable Action Execution card (public DTO only; never renders raw provider data). */
import React from 'react';
import { Alert, Button, Card, Descriptions, Space, Tag, Typography } from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  LoadingOutlined,
  StopOutlined,
} from '@ant-design/icons';
import type { WorkflowActionRecord } from '../../types/workflow';

interface Props extends WorkflowActionRecord {
  operationPending?: boolean;
  onRetry?: () => void;
  onReconcile?: () => void;
}

const STATUS_META: Record<WorkflowActionRecord['status'], {
  label: string; color: string; icon: React.ReactNode;
}> = {
  pending: { label: '等待执行', color: 'default', icon: null },
  running: { label: '执行中', color: 'processing', icon: <LoadingOutlined /> },
  executing: { label: '执行中', color: 'processing', icon: <LoadingOutlined /> },
  succeeded: { label: '执行成功', color: 'success', icon: <CheckCircleOutlined /> },
  failed: { label: '已确认执行失败', color: 'error', icon: <CloseCircleOutlined /> },
  unknown: { label: '执行结果待确认', color: 'warning', icon: <ExclamationCircleOutlined /> },
  cancelled: { label: '已取消', color: 'default', icon: <StopOutlined /> },
  blocked: { label: '已阻止', color: 'default', icon: <StopOutlined /> },
};

export const WorkflowActionRecordCard: React.FC<Props> = (props) => {
  const meta = STATUS_META[props.status] || STATUS_META.blocked;
  const isUnknown = props.status === 'unknown';

  return (
    <Card
      size="small"
      data-action-execution={props.actionExecutionId}
      data-action-status={props.status}
      style={{ marginBottom: 8, borderColor: isUnknown ? '#FAAD14' : undefined }}
      title={<span>{meta.icon} {props.actionType}</span>}
      extra={<Tag color={meta.color}>{meta.label}</Tag>}
    >
      {isUnknown && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="执行结果待确认"
          description="这不等于执行失败：请求可能已经到达外部系统，确认前 TrafficMind 不会再次执行。"
        />
      )}
      <Descriptions size="small" column={2}>
        <Descriptions.Item label="尝试次数">第 {props.attempt || 1} 次</Descriptions.Item>
        <Descriptions.Item label="Action ID">
          <Typography.Text code style={{ fontSize: 11 }}>{props.actionExecutionId}</Typography.Text>
        </Descriptions.Item>
        <Descriptions.Item label="开始时间">{props.startedAt || '-'}</Descriptions.Item>
        <Descriptions.Item label="结束时间">{props.finishedAt || '-'}</Descriptions.Item>
        <Descriptions.Item label="外部引用" span={2}>
          {props.externalReference
            ? <Typography.Text code style={{ fontSize: 11 }}>{props.externalReference}</Typography.Text>
            : '-'}
        </Descriptions.Item>
        {props.lastReconciledAt && (
          <Descriptions.Item label="最近确认" span={2}>{props.lastReconciledAt}</Descriptions.Item>
        )}
      </Descriptions>
      {props.message && (
        <Typography.Paragraph style={{ fontSize: 12, margin: '8px 0 0' }}>
          {props.message}
        </Typography.Paragraph>
      )}
      {props.error && (
        <Typography.Paragraph type="danger" style={{ fontSize: 12, margin: '8px 0 0' }}>
          原因：{props.error}
        </Typography.Paragraph>
      )}
      {isUnknown && !props.reconciliationSupported && (
        <Typography.Paragraph type="warning" style={{ fontSize: 12, margin: '8px 0 0' }}>
          该通道不支持自动确认，请人工核验；系统仍不会直接重试。
        </Typography.Paragraph>
      )}
      <Space style={{ marginTop: 10 }}>
        {props.operations.canReconcile && (
          <Button size="small" type="primary" disabled={props.operationPending} onClick={props.onReconcile}>
            确认外部执行结果
          </Button>
        )}
        {props.operations.canRetry && (
          <Button size="small" danger disabled={props.operationPending} onClick={props.onRetry}>
            重试 Action
          </Button>
        )}
      </Space>
    </Card>
  );
};
